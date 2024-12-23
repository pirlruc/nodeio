import unittest

import pydantic_core

import nodeio.decorators.validation


@nodeio.decorators.validation.custom_validate_call
def add_one_with_validate_call(value1: int, value2: int):
    return value1 + value2


@nodeio.decorators.validation.custom_validate_call(enabled=True)
def add_one_with_validate_call_enabled(value1: int, value2: int):
    return value1 + value2


@nodeio.decorators.validation.custom_validate_call(enabled=False)
def add_one_without_validate_call(value1: int, value2: int):
    return value1 + value2


class TestValidationDecorators(unittest.TestCase):
    def test_invalid_args_with_validate_call(self):
        with self.assertRaises(pydantic_core.ValidationError):
            add_one_with_validate_call(1, value2=3.1)

    def test_valid_args_with_validate_call(self):
        sum = add_one_with_validate_call(1, value2=3)
        self.assertEqual(sum, 4)

    def test_invalid_args_with_validate_call_enabled(self):
        with self.assertRaises(pydantic_core.ValidationError):
            add_one_with_validate_call_enabled(1, value2=3.1)

    def test_valid_args_with_validate_call_enabled(self):
        sum = add_one_with_validate_call_enabled(1, value2=3)
        self.assertEqual(sum, 4)

    def test_invalid_args_without_validate_call(self):
        sum = add_one_without_validate_call(1, value2=3.1)
        self.assertEqual(sum, 4.1)

    def test_valid_args_without_validate_call(self):
        sum = add_one_without_validate_call(1, value2=3)
        self.assertEqual(sum, 4)
