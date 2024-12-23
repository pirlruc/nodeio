import unittest
from inspect import Parameter, signature

import pydantic_core

from nodeio.engine.structures.arguments import InputArg, OutputArg


def no_input_no_output():
    print('a')


def no_input_with_output():
    return 3


def input_not_annotated(a, b=2):
    return 3


def input_annotated_invalid(a: int, b: float, c: int = 2.2):
    return a + b + c


def input_annotated(a: int, b: float, c: int = 2):
    return a + b + c


def input_output_annotated(a: int, b: int, c: int = 2) -> int:
    return a + b + c


class TestArguments(unittest.TestCase):
    def test_invalid_input_constructor(self):
        with self.assertRaises(pydantic_core.ValidationError):
            InputArg(key='')
            InputArg(key='  ')

    def test_input_constructor(self):
        input_1 = InputArg(key='a')
        input_2 = InputArg(key='b', type=str)
        input_3 = InputArg(key='c', type=str, default='test')
        self.assertEqual(input_1.key, 'a')
        self.assertIsNone(input_1.type)
        self.assertIsNone(input_1.default)
        self.assertEqual(input_2.key, 'b')
        self.assertEqual(input_2.type, str)
        self.assertIsNone(input_2.default)
        self.assertEqual(input_3.key, 'c')
        self.assertEqual(input_3.type, str)
        self.assertEqual(input_3.default, 'test')

    def test_input_modification(self):
        input = InputArg(key='a')
        input.type = Parameter.empty
        input.default = Parameter.empty
        self.assertIsNone(input.type)
        self.assertIsNone(input.default)

    def test_output_constructor(self):
        output_1 = OutputArg()
        output_2 = OutputArg(type=str)
        output_3 = OutputArg(type=Parameter.empty)
        self.assertIsNone(output_1.type)
        self.assertEqual(output_2.type, str)
        self.assertIsNone(output_3.type)

    def test_output_modification(self):
        output = OutputArg()
        output.type = str
        self.assertEqual(output.type, str)
        output.type = Parameter.empty
        self.assertIsNone(output.type)

    def test_no_input_no_output(self):
        sig = signature(no_input_no_output)
        self.assertEqual(len(sig.parameters.keys()), 0)
        output = OutputArg.from_return_annotation(sig.return_annotation)
        self.assertIsNone(output.type)

    def test_no_input_with_output(self):
        sig = signature(no_input_with_output)
        self.assertEqual(len(sig.parameters.keys()), 0)
        output = OutputArg.from_return_annotation(sig.return_annotation)
        self.assertIsNone(output.type)

    def test_input_not_annotated(self):
        sig = signature(input_not_annotated)
        input = {}
        for key in sig.parameters.keys():
            input[key] = InputArg.from_parameter(sig.parameters[key])
        output = OutputArg.from_return_annotation(sig.return_annotation)
        self.assertIsNone(output.type)
        self.assertEqual(input['a'].key, 'a')
        self.assertIsNone(input['a'].type)
        self.assertIsNone(input['a'].default)
        self.assertEqual(input['b'].key, 'b')
        self.assertEqual(input['b'].type, int)
        self.assertEqual(input['b'].default, 2)

    def test_input_annotated(self):
        sig = signature(input_annotated)
        input = {}
        for key in sig.parameters.keys():
            input[key] = InputArg.from_parameter(sig.parameters[key])
        output = OutputArg.from_return_annotation(sig.return_annotation)
        self.assertIsNone(output.type)
        self.assertEqual(input['a'].key, 'a')
        self.assertEqual(input['a'].type, int)
        self.assertIsNone(input['a'].default)
        self.assertEqual(input['b'].key, 'b')
        self.assertEqual(input['b'].type, float)
        self.assertIsNone(input['b'].default)
        self.assertEqual(input['c'].key, 'c')
        self.assertEqual(input['c'].type, int)
        self.assertEqual(input['c'].default, 2)

    def test_input_annotated_invalid(self):
        sig = signature(input_annotated_invalid)
        input = {}
        with self.assertRaises(TypeError):
            for key in sig.parameters.keys():
                input[key] = InputArg.from_parameter(sig.parameters[key])

    def test_input_output_annotated(self):
        sig = signature(input_output_annotated)
        input = {}
        for key in sig.parameters.keys():
            input[key] = InputArg.from_parameter(sig.parameters[key])
        output = OutputArg.from_return_annotation(sig.return_annotation)
        self.assertEqual(output.type, int)
        self.assertEqual(input['a'].key, 'a')
        self.assertEqual(input['a'].type, int)
        self.assertIsNone(input['a'].default)
        self.assertEqual(input['b'].key, 'b')
        self.assertEqual(input['b'].type, int)
        self.assertIsNone(input['b'].default)
        self.assertEqual(input['c'].key, 'c')
        self.assertEqual(input['c'].type, int)
        self.assertEqual(input['c'].default, 2)
