import asyncio
import os
import sys
import unittest

if sys.version_info.major >= 3 and sys.version_info.minor >= 11:
    from builtins import ExceptionGroup

from typing_extensions import Self

from nodeio.engine.base_node import BaseNode
from nodeio.engine.configuration import Graph, InputStream, Node
from nodeio.engine.graph_handler import GraphHandler
from nodeio.engine.node_factory import NodeFactory
from nodeio.engine.structures.stream import ContextStream
from nodeio.infrastructure.exceptions import ConfigurationError


class CompleteNodeNoInputs(BaseNode):
    def load(self) -> Self:
        return self

    def process(self) -> int:
        return 2


class CompleteNode(BaseNode):
    def load(self) -> Self:
        return self

    def process(self, x: int) -> int:
        return x + 2


class CompleteNodeOptions(BaseNode):
    y: int = 0
    z: int = 0

    def load(self, y: int, z: int) -> Self:
        self.y = y
        self.z = z
        return self

    def process(self, x: int = 4, w: int = 3) -> int:
        return x + self.y + self.z + 2 + w


class CompleteNodeError(BaseNode):
    y: int = 0
    z: int = 0

    def load(self, y: int, z: int) -> Self:
        self.y = y
        self.z = z
        return self

    def process(self, x: int = 4, w: int = 3) -> int:
        raise ValueError('Error test for async runs')
        return x + self.y + self.z + 2 + w


def create_complete_node_no_inputs() -> CompleteNodeNoInputs:
    return CompleteNodeNoInputs().load()


def create_complete_node() -> CompleteNode:
    return CompleteNode().load()


def create_complete_node_options(y: int, z: int) -> CompleteNodeOptions:
    return CompleteNodeOptions().load(y=y, z=z)


def create_complete_node_error(y: int, z: int) -> CompleteNodeError:
    return CompleteNodeError().load(y=y, z=z)


class TestGraphHandler(unittest.TestCase):
    def test_constructor(self):
        handler = GraphHandler()
        self.assertEqual(handler.number_output_streams, 0)
        self.assertEqual(handler.number_input_streams, 0)
        self.assertEqual(handler.number_nodes, 0)
        self.assertEqual(handler.number_main_processing_graph_nodes, 0)
        self.assertEqual(handler.number_secondary_processing_graph_nodes, 0)

    def test_duplicated_node(self):
        factory = NodeFactory()
        factory.register('node', create_complete_node)
        config = Graph(
            input_streams=['a'],
            output_streams=['b'],
            nodes=[
                Node(
                    node='a',
                    input_streams=[InputStream(arg='x', stream='a')],
                    output_stream='b',
                ),
                Node(
                    node='a',
                    input_streams=[InputStream(arg='x', stream='a')],
                    output_stream='c',
                ),
            ],
        )
        handler = GraphHandler()
        with self.assertRaises(ConfigurationError):
            handler.load(factory=factory, configuration=config)

    def test_main_output_stream_not_connected(self):
        factory = NodeFactory()
        factory.register('node', create_complete_node)
        config = Graph(
            input_streams=['a'],
            output_streams=['b'],
            nodes=[Node(node='a', input_streams=[InputStream(arg='x', stream='a')])],
        )
        handler = GraphHandler()
        with self.assertRaises(ConfigurationError):
            handler.load(factory=factory, configuration=config)

    def test_with_isolated_nodes(self):
        factory = NodeFactory()
        factory.register('node', create_complete_node_no_inputs)
        config = Graph(
            input_streams=['a'],
            output_streams=['b'],
            nodes=[
                Node(node='a', output_stream='b'),
                Node(node='b', output_stream='c'),
            ],
        )
        handler = GraphHandler()
        with self.assertRaises(ConfigurationError):
            handler.load(factory=factory, configuration=config)

    def test_with_unconnected_streams(self):
        factory = NodeFactory()
        factory.register('node', create_complete_node_no_inputs)
        config = Graph(
            input_streams=['a'],
            output_streams=['b'],
            nodes=[Node(node='a', output_stream='b')],
        )
        handler = GraphHandler()
        with self.assertRaises(ConfigurationError):
            handler.load(factory=factory, configuration=config)

    def test_config_without_options(self):
        factory = NodeFactory()
        factory.register('node', create_complete_node)
        config = Graph(
            input_streams=['a'],
            output_streams=['b'],
            nodes=[
                Node(
                    node='a',
                    input_streams=[InputStream(arg='x', stream='a')],
                    output_stream='b',
                )
            ],
        )
        handler = GraphHandler()
        handler.load(factory=factory, configuration=config)
        self.assertEqual(handler.number_output_streams, 1)
        self.assertEqual(handler.number_input_streams, 1)
        self.assertEqual(handler.number_nodes, 3)
        self.assertEqual(handler.number_main_processing_graph_nodes, 1)
        self.assertEqual(handler.number_secondary_processing_graph_nodes, 0)
        handler.write_graph(filename='test.png')
        self.assertTrue(os.path.isfile('test.png'))
        os.remove('test.png')

    def test_config_with_options(self):
        factory = NodeFactory()
        factory.register('node', create_complete_node_options)
        config = Graph(
            input_streams=['a'],
            output_streams=['b'],
            nodes=[
                Node(
                    node='a',
                    input_streams=[InputStream(arg='x', stream='a')],
                    output_stream='b',
                    options={'y': 2, 'z': 3},
                )
            ],
        )
        handler = GraphHandler()
        handler.load(factory=factory, configuration=config)
        self.assertEqual(handler.number_output_streams, 1)
        self.assertEqual(handler.number_input_streams, 1)
        self.assertEqual(handler.number_nodes, 3)
        self.assertEqual(handler.number_main_processing_graph_nodes, 1)
        self.assertEqual(handler.number_secondary_processing_graph_nodes, 0)

    def test_complex_graph(self):
        factory = NodeFactory()
        factory.register('node', create_complete_node_options)
        config = Graph(
            input_streams=['0'],
            output_streams=['1'],
            nodes=[
                Node(
                    node='a',
                    input_streams=[InputStream(arg='x', stream='0')],
                    output_stream='a',
                    options={'y': 2, 'z': 3},
                ),
                Node(node='h', output_stream='h', options={'y': 2, 'z': 3}),
                Node(
                    node='b',
                    input_streams=[InputStream(arg='x', stream='h')],
                    output_stream='b',
                    options={'y': 2, 'z': 3},
                ),
                Node(
                    node='c',
                    input_streams=[
                        InputStream(arg='x', stream='a'),
                        InputStream(arg='w', stream='b'),
                    ],
                    output_stream='c',
                    options={'y': 2, 'z': 3},
                ),
                Node(
                    node='d',
                    input_streams=[InputStream(arg='x', stream='c')],
                    output_stream='d',
                    options={'y': 2, 'z': 3},
                ),
                Node(
                    node='e',
                    input_streams=[
                        InputStream(arg='x', stream='c'),
                        InputStream(arg='w', stream='a'),
                    ],
                    output_stream='e',
                    options={'y': 2, 'z': 3},
                ),
                Node(
                    node='f',
                    input_streams=[
                        InputStream(arg='x', stream='d'),
                        InputStream(arg='w', stream='e'),
                    ],
                    output_stream='f',
                    options={'y': 2, 'z': 3},
                ),
                Node(
                    node='g',
                    input_streams=[InputStream(arg='x', stream='f')],
                    output_stream='1',
                    options={'y': 2, 'z': 3},
                ),
            ],
        )
        handler = GraphHandler()
        handler.load(factory=factory, configuration=config)
        self.assertEqual(handler.number_output_streams, 1)
        self.assertEqual(handler.number_input_streams, 1)
        self.assertEqual(handler.number_nodes, 10)
        self.assertEqual(handler.number_main_processing_graph_nodes, 6)
        self.assertEqual(handler.number_secondary_processing_graph_nodes, 2)
        handler.write_graph(filename='complex_graph.png')
        self.assertTrue(os.path.isfile('complex_graph.png'))
        os.remove('complex_graph.png')

        handler.open()

        context = {}
        context_stream = ContextStream(key='0')
        context_stream.register(new_value=1)
        context['0'] = context_stream
        output = handler.process(context=context)
        self.assertEqual(output['1'], 129)

    def test_process_before_open(self):
        factory = NodeFactory()
        factory.register('node', create_complete_node_options)
        config = Graph(
            input_streams=['0'],
            output_streams=['1'],
            nodes=[
                Node(
                    node='a',
                    input_streams=[InputStream(arg='x', stream='0')],
                    output_stream='a',
                    options={'y': 2, 'z': 3},
                ),
                Node(node='h', output_stream='h', options={'y': 2, 'z': 3}),
                Node(
                    node='b',
                    input_streams=[InputStream(arg='x', stream='h')],
                    output_stream='b',
                    options={'y': 2, 'z': 3},
                ),
                Node(
                    node='c',
                    input_streams=[
                        InputStream(arg='x', stream='a'),
                        InputStream(arg='w', stream='b'),
                    ],
                    output_stream='c',
                    options={'y': 2, 'z': 3},
                ),
                Node(
                    node='d',
                    input_streams=[InputStream(arg='x', stream='c')],
                    output_stream='d',
                    options={'y': 2, 'z': 3},
                ),
                Node(
                    node='e',
                    input_streams=[
                        InputStream(arg='x', stream='c'),
                        InputStream(arg='w', stream='a'),
                    ],
                    output_stream='e',
                    options={'y': 2, 'z': 3},
                ),
                Node(
                    node='f',
                    input_streams=[
                        InputStream(arg='x', stream='d'),
                        InputStream(arg='w', stream='e'),
                    ],
                    output_stream='f',
                    options={'y': 2, 'z': 3},
                ),
                Node(
                    node='g',
                    input_streams=[InputStream(arg='x', stream='f')],
                    output_stream='1',
                    options={'y': 2, 'z': 3},
                ),
            ],
        )
        handler = GraphHandler()
        handler.load(factory=factory, configuration=config)
        self.assertEqual(handler.number_output_streams, 1)
        self.assertEqual(handler.number_input_streams, 1)
        self.assertEqual(handler.number_nodes, 10)
        self.assertEqual(handler.number_main_processing_graph_nodes, 6)
        self.assertEqual(handler.number_secondary_processing_graph_nodes, 2)
        with self.assertRaises(RuntimeError):
            handler.process()

    def test_process_without_input_streams_in_context(self):
        factory = NodeFactory()
        factory.register('node', create_complete_node_options)
        config = Graph(
            input_streams=['0'],
            output_streams=['1'],
            nodes=[
                Node(
                    node='a',
                    input_streams=[InputStream(arg='x', stream='0')],
                    output_stream='a',
                    options={'y': 2, 'z': 3},
                ),
                Node(
                    node='g',
                    input_streams=[InputStream(arg='x', stream='a')],
                    output_stream='1',
                    options={'y': 2, 'z': 3},
                ),
            ],
        )
        handler = GraphHandler()
        handler.load(factory=factory, configuration=config)
        self.assertEqual(handler.number_output_streams, 1)
        self.assertEqual(handler.number_input_streams, 1)
        self.assertEqual(handler.number_nodes, 4)
        self.assertEqual(handler.number_main_processing_graph_nodes, 2)
        self.assertEqual(handler.number_secondary_processing_graph_nodes, 0)
        handler.open()
        with self.assertRaises(KeyError):
            handler.process()
        with self.assertRaises(KeyError):
            context_stream = ContextStream(key='0')
            context = {}
            context['0'] = context_stream
            handler.process(context=context)

    # def test_process_without_output_streams_in_context(self):
    #     # Not possible to obtain since this is ensured by validation on the creation of the graph.
    #     # To test comment the block of code in graph_handler corresponding to the processing of the main processing graph.
    #     factory = NodeFactory()
    #     factory.register("node", create_complete_node_options)
    #     config = Graph(
    #         input_streams=["0"],
    #         output_streams=["1"],
    #         nodes=[
    #             Node(node="a",input_streams=[InputStream(arg="x",stream="0")],output_stream="a",options={"y": 2, "z": 3}),
    #             Node(node="g",input_streams=[InputStream(arg="x",stream="a")],output_stream="1",options={"y": 2, "z": 3}),
    #             ])
    #     handler = GraphHandler()
    #     handler.load(factory=factory, configuration=config)
    #     handler.open()

    #     context = {}
    #     context_stream = ContextStream(key="0")
    #     context_stream.register(new_value=1)
    #     context["0"] = context_stream
    #     with self. assertRaises(KeyError):
    #         handler.process(context)

    def test_validate_output(self):
        factory = NodeFactory()
        factory.register('node', create_complete_node_options)
        config = Graph(
            input_streams=['0'],
            output_streams=['1'],
            nodes=[
                Node(
                    node='a',
                    input_streams=[InputStream(arg='x', stream='0')],
                    output_stream='a',
                    options={'y': 2, 'z': 3},
                ),
                Node(
                    node='g',
                    input_streams=[InputStream(arg='x', stream='a')],
                    output_stream='1',
                    options={'y': 2, 'z': 3},
                ),
            ],
        )
        handler = GraphHandler()
        handler.load(factory=factory, configuration=config)
        self.assertEqual(handler.number_output_streams, 1)
        self.assertEqual(handler.number_input_streams, 1)
        self.assertEqual(handler.number_nodes, 4)
        self.assertEqual(handler.number_main_processing_graph_nodes, 2)
        self.assertEqual(handler.number_secondary_processing_graph_nodes, 0)
        context = {}
        context_stream = ContextStream(key='0')
        context_stream.register(new_value=1)
        context['0'] = context_stream
        output = handler.process(context=context)
        self.assertEqual(output['1'], 21)

    def test_process_async_before_open(self):
        factory = NodeFactory()
        factory.register('node', create_complete_node_options)
        config = Graph(
            input_streams=['0'],
            output_streams=['1'],
            nodes=[
                Node(
                    node='a',
                    input_streams=[InputStream(arg='x', stream='0')],
                    output_stream='a',
                    options={'y': 2, 'z': 3},
                ),
                Node(node='h', output_stream='h', options={'y': 2, 'z': 3}),
                Node(
                    node='b',
                    input_streams=[InputStream(arg='x', stream='h')],
                    output_stream='b',
                    options={'y': 2, 'z': 3},
                ),
                Node(
                    node='c',
                    input_streams=[
                        InputStream(arg='x', stream='a'),
                        InputStream(arg='w', stream='b'),
                    ],
                    output_stream='c',
                    options={'y': 2, 'z': 3},
                ),
                Node(
                    node='d',
                    input_streams=[InputStream(arg='x', stream='c')],
                    output_stream='d',
                    options={'y': 2, 'z': 3},
                ),
                Node(
                    node='e',
                    input_streams=[
                        InputStream(arg='x', stream='c'),
                        InputStream(arg='w', stream='a'),
                    ],
                    output_stream='e',
                    options={'y': 2, 'z': 3},
                ),
                Node(
                    node='f',
                    input_streams=[
                        InputStream(arg='x', stream='d'),
                        InputStream(arg='w', stream='e'),
                    ],
                    output_stream='f',
                    options={'y': 2, 'z': 3},
                ),
                Node(
                    node='g',
                    input_streams=[InputStream(arg='x', stream='f')],
                    output_stream='1',
                    options={'y': 2, 'z': 3},
                ),
            ],
        )
        handler = GraphHandler()
        handler.load(factory=factory, configuration=config)
        self.assertEqual(handler.number_output_streams, 1)
        self.assertEqual(handler.number_input_streams, 1)
        self.assertEqual(handler.number_nodes, 10)
        self.assertEqual(handler.number_main_processing_graph_nodes, 6)
        self.assertEqual(handler.number_secondary_processing_graph_nodes, 2)
        with self.assertRaises(RuntimeError):
            asyncio.run(handler.process_async())

    def test_complex_graph_async(self):
        factory = NodeFactory()
        factory.register('node', create_complete_node_options)
        config = Graph(
            input_streams=['0'],
            output_streams=['1'],
            nodes=[
                Node(
                    node='a',
                    input_streams=[InputStream(arg='x', stream='0')],
                    output_stream='a',
                    options={'y': 2, 'z': 3},
                ),
                Node(node='h', output_stream='h', options={'y': 2, 'z': 3}),
                Node(
                    node='b',
                    input_streams=[InputStream(arg='x', stream='h')],
                    output_stream='b',
                    options={'y': 2, 'z': 3},
                ),
                Node(
                    node='c',
                    input_streams=[
                        InputStream(arg='x', stream='a'),
                        InputStream(arg='w', stream='b'),
                    ],
                    output_stream='c',
                    options={'y': 2, 'z': 3},
                ),
                Node(
                    node='d',
                    input_streams=[InputStream(arg='x', stream='c')],
                    output_stream='d',
                    options={'y': 2, 'z': 3},
                ),
                Node(
                    node='e',
                    input_streams=[
                        InputStream(arg='x', stream='c'),
                        InputStream(arg='w', stream='a'),
                    ],
                    output_stream='e',
                    options={'y': 2, 'z': 3},
                ),
                Node(
                    node='f',
                    input_streams=[
                        InputStream(arg='x', stream='d'),
                        InputStream(arg='w', stream='e'),
                    ],
                    output_stream='f',
                    options={'y': 2, 'z': 3},
                ),
                Node(
                    node='g',
                    input_streams=[InputStream(arg='x', stream='f')],
                    output_stream='1',
                    options={'y': 2, 'z': 3},
                ),
            ],
        )
        handler = GraphHandler()
        handler.load(factory=factory, configuration=config)
        self.assertEqual(handler.number_output_streams, 1)
        self.assertEqual(handler.number_input_streams, 1)
        self.assertEqual(handler.number_nodes, 10)
        self.assertEqual(handler.number_main_processing_graph_nodes, 6)
        self.assertEqual(handler.number_secondary_processing_graph_nodes, 2)
        handler.write_graph(filename='complex_graph.png')
        self.assertTrue(os.path.isfile('complex_graph.png'))
        os.remove('complex_graph.png')

        asyncio.run(handler.open_async())

        context = {}
        context_stream = ContextStream(key='0')
        context_stream.register(new_value=1)
        context['0'] = context_stream
        output = asyncio.run(handler.process_async(context=context))
        self.assertEqual(output['1'], 129)

    def test_process_async_with_error(self):
        factory = NodeFactory()
        factory.register('node', create_complete_node_error)
        config = Graph(
            input_streams=['0'],
            output_streams=['1'],
            nodes=[
                Node(
                    node='a',
                    input_streams=[InputStream(arg='x', stream='0')],
                    output_stream='a',
                    options={'y': 2, 'z': 3},
                ),
                Node(
                    node='b',
                    input_streams=[InputStream(arg='x', stream='a')],
                    output_stream='1',
                    options={'y': 2, 'z': 3},
                ),
            ],
        )
        handler = GraphHandler()
        handler.load(factory=factory, configuration=config)
        self.assertEqual(handler.number_output_streams, 1)
        self.assertEqual(handler.number_input_streams, 1)
        self.assertEqual(handler.number_nodes, 4)
        self.assertEqual(handler.number_main_processing_graph_nodes, 2)
        self.assertEqual(handler.number_secondary_processing_graph_nodes, 0)

        asyncio.run(handler.open_async())

        context = {}
        context_stream = ContextStream(key='0')
        context_stream.register(new_value=1)
        context['0'] = context_stream
        if sys.version_info.major >= 3 and sys.version_info.minor >= 11:
            with self.assertRaises(ExceptionGroup):
                asyncio.run(handler.process_async(context=context))
        else:
            with self.assertRaises(ValueError):
                asyncio.run(handler.process_async(context=context))
