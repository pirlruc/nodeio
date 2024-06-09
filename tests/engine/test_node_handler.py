import unittest

import networkx
import pydantic_core
from typing_extensions import Self

from nodeio.engine.base_node import BaseNode
from nodeio.engine.configuration import InputStream, Node
from nodeio.engine.node_handler import NodeHandler
from nodeio.engine.stream import ContextStream, OutputStream
from nodeio.engine.stream_handler import StreamHandler
from nodeio.infrastructure.exceptions import ConfigurationError


class CompleteNode(BaseNode):
    def load(self) -> Self:
        return self

    def process(self, x: int = 2) -> int:
        return x + 2


class CompleteNodeOptions(BaseNode):
    y: int = 0
    z: int = 0

    def load(self, y: int, z: int) -> Self:
        self.y = y
        self.z = z
        return self

    def process(self, x: int) -> int:
        return x + self.y + self.z + 2


def create_complete_node() -> CompleteNode:
    return CompleteNode().load()


def create_complete_node_options(y: int, z: int) -> CompleteNode:
    return CompleteNodeOptions().load(y=y, z=z)


class TestNodeHandler(unittest.TestCase):
    def test_invalid_constructor(self):
        with self.assertRaises(pydantic_core.ValidationError):
            NodeHandler()
        with self.assertRaises(pydantic_core.ValidationError):
            NodeHandler(name="a")
        with self.assertRaises(pydantic_core.ValidationError):
            NodeHandler(functor=create_complete_node)
        with self.assertRaises(pydantic_core.ValidationError):
            NodeHandler(name="", functor=create_complete_node)
        with self.assertRaises(pydantic_core.ValidationError):
            NodeHandler(name="  ", functor=create_complete_node)
        with self.assertRaises(pydantic_core.ValidationError):
            NodeHandler(name="  ", functor=create_complete_node)
        with self.assertRaises(pydantic_core.ValidationError):
            NodeHandler(name="a", functor=1)

    def test_invalid_functor(self):
        def create_abstract_node(a: int) -> BaseNode:
            return a

        with self.assertRaises(TypeError):
            NodeHandler(name="a", functor=create_abstract_node)

    def test_constructor(self):
        def no_output(a: int):
            return a

        handler_1 = NodeHandler(name="a", functor=create_complete_node)
        handler_2 = NodeHandler(name="b", functor=create_complete_node_options)
        handler_3 = NodeHandler(name="c", functor=no_output)
        self.assertEqual(handler_1.name, "a")
        self.assertEqual(handler_1.functor, create_complete_node)
        self.assertEqual(handler_1.number_inputs, 0)
        self.assertTrue(handler_1.has_output())
        self.assertEqual(handler_1.number_input_streams, 0)
        self.assertFalse(handler_1.has_output_stream())
        self.assertEqual(handler_2.name, "b")
        self.assertEqual(handler_2.functor, create_complete_node_options)
        self.assertEqual(handler_2.number_inputs, 2)
        self.assertTrue(handler_2.has_output())
        self.assertEqual(handler_2.number_input_streams, 0)
        self.assertFalse(handler_2.has_output_stream())
        self.assertEqual(handler_3.name, "c")
        self.assertEqual(handler_3.functor, no_output)
        self.assertEqual(handler_3.number_inputs, 1)
        self.assertFalse(handler_3.has_output())
        self.assertEqual(handler_3.number_input_streams, 0)
        self.assertFalse(handler_3.has_output_stream())

    def test_invalid_identifier(self):
        stream_handler = StreamHandler(graph=networkx.DiGraph())
        handler = NodeHandler(name="a", functor=create_complete_node)
        with self.assertRaises(RuntimeError):
            handler.load(stream_handler=stream_handler,
                         configuration=Node(node="b", output_stream="2"))

    def test_no_input_streams_with_mandatory_inputs(self):
        def mandatory_inputs(a: int):
            return a

        stream_handler = StreamHandler(graph=networkx.DiGraph())
        handler = NodeHandler(name="a", functor=mandatory_inputs)
        with self.assertRaises(ConfigurationError):
            handler.load(stream_handler=stream_handler,
                         configuration=Node(node="a", output_stream="2"))

    def test_no_input_arg(self):
        def mandatory_inputs(a: int):
            return a

        stream_handler = StreamHandler(graph=networkx.DiGraph())
        handler = NodeHandler(name="a", functor=mandatory_inputs)
        with self.assertRaises(ConfigurationError):
            handler.load(stream_handler=stream_handler, configuration=Node(
                node="a", input_streams=[InputStream(arg="b", stream="1")],
                output_stream="2")
            )

    def test_no_output_stream(self):
        def mandatory_inputs(a: int):
            return a

        stream_handler = StreamHandler(graph=networkx.DiGraph())
        handler = NodeHandler(name="a", functor=mandatory_inputs)
        with self.assertRaises(ConfigurationError):
            handler.load(stream_handler=stream_handler, configuration=Node(
                node="a", input_streams=[InputStream(arg="a", stream="1")],
                output_stream="2")
            )

    def test_load(self):
        stream_handler = StreamHandler(graph=networkx.DiGraph())
        handler_1 = NodeHandler(name="a", functor=create_complete_node)
        stream_handler.add_output_stream(
            OutputStream(key="3"), origin="source")
        stream_handler.add_output_stream(
            OutputStream(key="4"), origin="source")
        handler_2 = NodeHandler(name="b", functor=create_complete_node_options)
        handler_1.load(stream_handler=stream_handler,
                       configuration=Node(node="a", output_stream="1"))
        handler_2.load(stream_handler=stream_handler,
                       configuration=Node(node="b", input_streams=[
                           InputStream(arg="y", stream="3"),
                           InputStream(arg="z", stream="4")
                       ], output_stream="2")
                       )
        self.assertEqual(handler_1.name, "a")
        self.assertEqual(handler_1.functor, create_complete_node)
        self.assertEqual(handler_1.number_inputs, 0)
        self.assertTrue(handler_1.has_output())
        self.assertEqual(handler_1.number_input_streams, 0)
        self.assertTrue(handler_1.has_output_stream())
        self.assertEqual(handler_2.name, "b")
        self.assertEqual(handler_2.functor, create_complete_node_options)
        self.assertEqual(handler_2.number_inputs, 2)
        self.assertTrue(handler_2.has_output())
        self.assertEqual(handler_2.number_input_streams, 2)
        self.assertTrue(handler_2.has_output_stream())

    def test_process_without_options(self):
        stream_handler = StreamHandler(graph=networkx.DiGraph())
        handler = NodeHandler(name="a", functor=CompleteNode().process)
        handler.load(stream_handler=stream_handler,
                     configuration=Node(node="a", output_stream="1"))
        context = handler.process()
        self.assertEqual(context["1"].get(), 4)

    def test_process_with_options(self):
        stream_handler = StreamHandler(graph=networkx.DiGraph())
        stream_handler.add_output_stream(
            OutputStream(key="1"), origin="source")
        handler = NodeHandler(
            name="b", functor=CompleteNodeOptions().load(y=2, z=3).process)
        handler.load(stream_handler=stream_handler, configuration=Node(
            node="b", input_streams=[InputStream(arg="x", stream="1")],
            output_stream="2")
        )
        context: dict[str, ContextStream] = {}
        context["1"] = ContextStream(key="1", type=int)
        context["1"].register(new_value=1)
        context = handler.process(context)
        self.assertEqual(context["2"].get(), 8)
