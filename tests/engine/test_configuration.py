import unittest

import pydantic_core

from nodeio.engine.configuration import (
    DictAction,
    Graph,
    InputStream,
    ListAction,
    Node,
)


class TestListAction(unittest.TestCase):
    def test_invalid_constructor(self):
        with self.assertRaises(pydantic_core.ValidationError):
            ListAction(index=1.3)
            ListAction(index="a")

    def test_invalid_update(self):
        action = ListAction(index=1)
        with self.assertRaises(pydantic_core.ValidationError):
            action.index = 1.3

    def test_constructor(self):
        action = ListAction(index=1)
        self.assertEqual(action.index, 1)

    def test_update(self):
        action = ListAction(index=1)
        action.index = 2
        self.assertEqual(action.index, 2)

    def test_json(self):
        action = ListAction(index=1)
        self.assertEqual(ListAction.model_validate_json(
            action.model_dump_json()), action)


class TestDictAction(unittest.TestCase):
    def test_invalid_constructor(self):
        with self.assertRaises(pydantic_core.ValidationError):
            DictAction(key=1)
            DictAction(key="")
            DictAction(key="  ")

    def test_invalid_update(self):
        action = DictAction(key="a")
        with self.assertRaises(pydantic_core.ValidationError):
            action.key = 1
            action.key = ""
            action.key = "  "

    def test_constructor(self):
        action = DictAction(key="a")
        self.assertEqual(action.key, "a")

    def test_update(self):
        action = DictAction(key="a")
        action.key = "b"
        self.assertEqual(action.key, "b")

    def test_json(self):
        action = DictAction(key="a")
        self.assertEqual(DictAction.model_validate_json(
            action.model_dump_json()), action)


class TestInputStream(unittest.TestCase):
    def test_invalid_constructor(self):
        with self.assertRaises(pydantic_core.ValidationError):
            InputStream()
            InputStream(arg=1)
            InputStream(arg="a")
            InputStream(stream=2)
            InputStream(stream="b")

    def test_invalid_data_type(self):
        with self.assertRaises(pydantic_core.ValidationError):
            InputStream(arg=1, stream=2)
            InputStream(arg=1, stream="a")
            InputStream(arg="a", stream=2)
            InputStream(arg="a", stream="b", actions=[1, ListAction(index=1)])

    def test_invalid_key(self):
        with self.assertRaises(pydantic_core.ValidationError):
            InputStream(arg="", stream="b")
            InputStream(arg="  ", stream="b")
            InputStream(arg="a", stream="")
            InputStream(arg="a", stream="  ")

    def test_constructor(self):
        stream_1 = InputStream(arg=" a ", stream="b  ")
        stream_2 = InputStream(arg="a", stream="b")
        stream_3 = InputStream(arg="c", stream="d", actions=[
                               DictAction(key="e"), ListAction(index=1)])
        self.assertEqual(stream_1.arg, "a")
        self.assertEqual(stream_1.stream, "b")
        self.assertEqual(stream_1.actions, list())
        self.assertEqual(stream_2.arg, "a")
        self.assertEqual(stream_2.stream, "b")
        self.assertEqual(stream_2.actions, list())
        self.assertEqual(stream_3.arg, "c")
        self.assertEqual(stream_3.stream, "d")
        self.assertEqual(stream_3.actions, [
                         DictAction(key="e"), ListAction(index=1)])

    def test_json(self):
        stream_1 = InputStream(arg="a", stream="b")
        stream_2 = InputStream(arg="c", stream="d", actions=[
                               DictAction(key="e"), ListAction(index=1)])
        self.assertEqual(InputStream.model_validate_json(
            stream_1.model_dump_json()), stream_1)
        self.assertEqual(InputStream.model_validate_json(
            stream_2.model_dump_json()), stream_2)


class TestNode(unittest.TestCase):
    def test_invalid_constructor(self):
        with self.assertRaises(pydantic_core.ValidationError):
            Node()
            Node(input_streams=1)
            Node(output_stream=2)

    def test_invalid_data_type(self):
        with self.assertRaises(pydantic_core.ValidationError):
            Node(node=1)
            Node(node="a", input_streams=1)
            Node(node="a", output_stream=2)
            Node(node="a", input_streams=[
                 {"arg": "a", "stream": "b"}], output_stream=2)
            Node(node="a", input_streams=[
                 {"arg": "a", "stream": "b"}], output_stream="a", options=123)

    def test_invalid_key(self):
        with self.assertRaises(pydantic_core.ValidationError):
            Node(node="")
            Node(node="  ")

    def test_invalid_config(self):
        with self.assertRaises(ValueError):
            Node(node=" a ")
        with self.assertRaises(ValueError):
            Node(node="a", input_streams=[])

    def test_constructor(self):
        node_1 = Node(node="b", output_stream="2")
        node_2 = Node(node="c", input_streams=[{"arg": "3", "stream": "4"}])
        node_3 = Node(node="d", input_streams=[InputStream(
            arg="5", stream="6")], output_stream="2")
        node_4 = Node(node="e", input_streams=[], output_stream="3")
        node_5 = Node(node="f", output_stream="4", options={"a": "test"})
        self.assertEqual(node_1.node, "b")
        self.assertEqual(node_1.type, "node")
        self.assertEqual(node_1.input_streams, list())
        self.assertEqual(node_1.output_stream, "2")
        self.assertEqual(node_1.options, dict())
        self.assertEqual(node_2.node, "c")
        self.assertEqual(node_2.type, "node")
        self.assertEqual(node_2.input_streams[0].arg, "3")
        self.assertEqual(node_2.input_streams[0].stream, "4")
        self.assertIsNone(node_2.output_stream)
        self.assertEqual(node_2.options, dict())
        self.assertEqual(node_3.node, "d")
        self.assertEqual(node_3.type, "node")
        self.assertEqual(node_3.input_streams[0].arg, "5")
        self.assertEqual(node_3.input_streams[0].stream, "6")
        self.assertEqual(node_3.output_stream, "2")
        self.assertEqual(node_3.options, dict())
        self.assertEqual(node_4.node, "e")
        self.assertEqual(node_4.type, "node")
        self.assertEqual(node_4.input_streams, list())
        self.assertEqual(node_4.output_stream, "3")
        self.assertEqual(node_4.options, dict())
        self.assertEqual(node_5.node, "f")
        self.assertEqual(node_5.type, "node")
        self.assertEqual(node_5.input_streams, list())
        self.assertEqual(node_5.output_stream, "4")
        self.assertEqual(node_5.options, {"a": "test"})

    def test_json(self):
        node_1_ori = Node(node="d", input_streams=[
                          InputStream(arg="5", stream="6")], output_stream="2")
        node_2_ori = Node(node="e", input_streams=[InputStream(
            arg="1", stream="2")], output_stream="3", options={"4": "test"})
        print(node_1_ori)
        print(node_1_ori.model_dump_json())
        print(Node.model_validate_json(node_1_ori.model_dump_json()))
        self.assertEqual(Node.model_validate_json(
            node_1_ori.model_dump_json()), node_1_ori)
        self.assertEqual(Node.model_validate_json(
            node_2_ori.model_dump_json()), node_2_ori)


class TestGraph(unittest.TestCase):
    def test_invalid_constructor(self):
        with self.assertRaises(pydantic_core.ValidationError):
            Graph()
            Graph(input_streams=1)
            Graph(output_streams=2)
            Graph(nodes=3)

    def test_invalid_data_type(self):
        with self.assertRaises(pydantic_core.ValidationError):
            Graph(nodes=[], input_streams=["a", "b"], output_streams=3)
            Graph(nodes=[], input_streams=2, output_streams="c")
            Graph(nodes=[{"node": "a"}], input_streams=2, output_streams="c")
            Graph(nodes=[{"node": "a"}], input_streams="b", output_streams="c")

    def test_invalid_key(self):
        with self.assertRaises(pydantic_core.ValidationError):
            Graph(nodes=[{"node": ""}], input_streams=[
                  "b"], output_streams=["c"])
            Graph(nodes=[{"node": "  "}], input_streams=[
                  "b"], output_streams=["c"])
            Graph(nodes=[{"node": "a"}], input_streams=[
                  ""], output_streams=["c"])
            Graph(nodes=[{"node": "a"}], input_streams=[
                  "  "], output_streams=["c"])
            Graph(nodes=[{"node": "a"}], input_streams=[
                  "b"], output_streams=[""])
            Graph(nodes=[{"node": "a"}], input_streams=[
                  "b"], output_streams=["  "])

    def test_at_least_one_ocurrence(self):
        with self.assertRaises(ValueError):
            Graph(nodes=[], input_streams=["b"], output_streams=["c"])
        with self.assertRaises(ValueError):
            Graph(nodes=[{"node": "a", "type": "node"}],
                  input_streams=[], output_streams=["c"])
        with self.assertRaises(ValueError):
            Graph(nodes=[{"node": "a", "type": "node"}],
                  input_streams=["b"], output_streams=[])

    def test_constructor(self):
        graph_1 = Graph(nodes=[{"node": "a", "type": "node", "output_stream": "1"}], input_streams=[
                        "b"], output_streams=["c"])
        graph_2 = Graph(nodes=[{"node": "a", "type": "node", "output_stream": "1"}, Node(
            node="b", output_stream="2")], input_streams=["b", "d"], output_streams=["c", "e"])
        self.assertEqual(len(graph_1.nodes), 1)
        self.assertEqual(len(graph_1.input_streams), 1)
        self.assertEqual(len(graph_1.output_streams), 1)
        self.assertEqual(len(graph_2.nodes), 2)
        self.assertEqual(len(graph_2.input_streams), 2)
        self.assertEqual(len(graph_2.output_streams), 2)

    def test_json(self):
        graph_1_ori = Graph(nodes=[{"node": "a", "type": "node", "output_stream": "1"}], input_streams=[
                            "b"], output_streams=["c"])
        graph_2_ori = Graph(nodes=[{"node": "a", "type": "node", "output_stream": "1"}, Node(
            node="b", output_stream="2")], input_streams=["b", "d"], output_streams=["c", "e"])
        graph_1_par = Graph.model_validate_json(graph_1_ori.model_dump_json())
        graph_2_par = Graph.model_validate_json(graph_2_ori.model_dump_json())
        self.assertEqual(len(graph_1_par.nodes), 1)
        self.assertEqual(len(graph_1_par.input_streams), 1)
        self.assertEqual(len(graph_1_par.output_streams), 1)
        self.assertEqual(graph_1_par, graph_1_ori)
        self.assertEqual(len(graph_2_par.nodes), 2)
        self.assertEqual(len(graph_2_par.input_streams), 2)
        self.assertEqual(len(graph_2_par.output_streams), 2)
        self.assertEqual(graph_2_par, graph_2_ori)
