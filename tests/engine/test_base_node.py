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
            CompleteNode(id="")
            CompleteNode(id="  ")

    def test_invalid_update(self):
        node = CompleteNode()
        with self.assertRaises(pydantic_core.ValidationError):
            node.id=""
            node.id="  "

    def test_complete_node(self):
        node = CompleteNode()
        self.assertEqual(node.id,"completenode")
        self.assertEqual(node.process(),4)
        node.id = "test"
        self.assertEqual(node.id,'test')
