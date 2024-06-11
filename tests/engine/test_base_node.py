import unittest

import pydantic_core

from nodeio.engine.base_node import BaseNode


class IncompleteNode(BaseNode):
    pass


class CompleteNode(BaseNode):
    def load(self):
        return self

    def process(self):
        return 2 + 2


class TestBaseNode(unittest.TestCase):
    def test_incomplete_node(self):
        with self.assertRaises(TypeError):
            IncompleteNode()

    def test_invalid_constructor(self):
        with self.assertRaises(pydantic_core.ValidationError):
            CompleteNode(name="")
            CompleteNode(name="  ")

    def test_invalid_update(self):
        node = CompleteNode()
        with self.assertRaises(pydantic_core.ValidationError):
            node.name = ""
            node.name = "  "

    def test_complete_node(self):
        node = CompleteNode()
        self.assertEqual(node.name, "completenode")
        self.assertEqual(node.process(), 4)
        node.name = "test"
        self.assertEqual(node.name, 'test')

    def test_complete_node_with_name(self):
        node = CompleteNode(name="test")
        self.assertEqual(node.name, "test")
        self.assertEqual(node.process(), 4)
