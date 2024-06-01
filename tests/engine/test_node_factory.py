import unittest
import pydantic_core
from typing import Union
from typing_extensions import Self
from nodeio.engine.base_node import BaseNode
from nodeio.engine.node_factory import NodeFactory

class AddNodeA(BaseNode):
    def load(self) -> Self:
        return self

    def process(self):
        return 1 + 2

class AddNodeB(BaseNode):
    def load(self):
        return self

    def process(self):
        return 3 + 4

def create_add_node_a() -> AddNodeA:
    return AddNodeA().load()

def create_add_node_b() -> AddNodeB:
    return AddNodeB().load()

class TestNodeFactory(unittest.TestCase):
    def test_empty_constructor(self):
        factory = NodeFactory()
        self.assertEqual(factory.number_callbacks,0)

    def test_none_functor(self):
        factory = NodeFactory()
        with self.assertRaises(pydantic_core._pydantic_core.ValidationError):
            factory.register("a", None)

    def test_no_callable_functor(self):
        factory = NodeFactory()
        with self.assertRaises(pydantic_core._pydantic_core.ValidationError):
            factory.register("a","test")

    def test_register_empty_key(self):
        factory = NodeFactory()
        with self.assertRaises(pydantic_core._pydantic_core.ValidationError):
            factory.register("","test")
            factory.register(" ","test")
            
    def test_functor_multiple_outputs(self):
        def create_union(x: int, y: int) -> Union[int,NodeFactory]:
            return "invalid"
        
        factory = NodeFactory()
        with self.assertRaises(TypeError):
            factory.register("a", create_union)

    def test_functor_invalid_output(self):
        def create_str(x: int, y: int) -> str:
            return "invalid"
        
        factory = NodeFactory()
        with self.assertRaises(TypeError):
            factory.register("a", create_str)

    def test_functor_abstract_output(self):
        def create_str(x: int, y: int) -> BaseNode:
            return "invalid"
        
        factory = NodeFactory()
        with self.assertRaises(TypeError):
            factory.register("a", create_str)

    def test_functor_no_node_output(self):
        factory = NodeFactory()
        with self.assertRaises(TypeError):
            factory.register("a", lambda: 1)

    def test_functor_single_output(self):
        def create_invalid_base_node(x: int, y: int) -> AddNodeA:
            return "invalid"
        
        factory = NodeFactory()
        factory.register("a", create_invalid_base_node)
        self.assertEqual(factory.number_callbacks,1)

    def test_duplicated_key(self):
        def create_invalid_base_node(x: int, y: int) -> AddNodeA:
            return "invalid"

        factory = NodeFactory()
        factory.register("a",create_invalid_base_node)
        with self.assertRaises(KeyError):
            factory.register("a",create_invalid_base_node)

    def test_remove_empty_key(self):
        factory = NodeFactory()
        with self.assertRaises(pydantic_core._pydantic_core.ValidationError):
            factory.unregister("","test")
            factory.unregister(" ","test")
            
    def test_remove_non_existing_key(self):
        def create_invalid_base_node(x: int, y: int) -> AddNodeA:
            return "invalid"

        factory = NodeFactory()
        factory.register("a",create_invalid_base_node)
        with self.assertRaises(KeyError):
            factory.unregister("b")

    def test_remove_existing_key(self):
        def create_invalid_base_node(x: int, y: int) -> AddNodeA:
            return "invalid"

        factory = NodeFactory()
        factory.register("a",create_invalid_base_node)
        factory.unregister("a")
        self.assertEqual(factory.number_callbacks,0)

    def test_create_empty_key(self):
        factory = NodeFactory()
        with self.assertRaises(pydantic_core._pydantic_core.ValidationError):
            factory.create("","test")
            factory.create(" ","test")
            
    def test_create_non_existing_key(self):
        def create_invalid_base_node(x: int, y: int) -> AddNodeA:
            return "invalid"

        factory = NodeFactory()
        factory.register("a",create_invalid_base_node)
        with self.assertRaises(KeyError):
            factory.create("b")

    def test_create_existing_key(self):
        factory = NodeFactory()
        factory.register("a",create_add_node_a)
        node_a = factory.create("a")
        self.assertEqual(node_a.process(),3)

    def test_create_not_base_node(self):
        def create_invalid_base_node(x: int, y: int) -> AddNodeA:
            return "invalid"
            
        factory = NodeFactory()
        factory.register("a",create_invalid_base_node)
        with self.assertRaises(TypeError):
            factory.create("a",2,2)

    def test_create_multiple_nodes(self):
        factory = NodeFactory()
        factory.register("a",AddNodeA().load)
        factory.register("b",create_add_node_b)
        node_a = factory.create("a")
        node_b = factory.create("b")
        self.assertEqual(node_a.process(),3)
        self.assertEqual(node_b.process(),7)
