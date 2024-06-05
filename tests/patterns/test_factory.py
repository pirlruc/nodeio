import unittest

import pydantic_core

from nodeio.patterns.factory import Factory


class TestFactory(unittest.TestCase):
    def test_empty_constructor(self):
        factory = Factory()
        self.assertEqual(factory.number_callbacks, 0)

    def test_none_functor(self):
        factory = Factory()
        with self.assertRaises(pydantic_core._pydantic_core.ValidationError):
            factory.register("a", None)

    def test_no_callable_functor(self):
        factory = Factory()
        with self.assertRaises(pydantic_core._pydantic_core.ValidationError):
            factory.register("a", "test")

    def test_register_empty_key(self):
        factory = Factory()
        with self.assertRaises(pydantic_core._pydantic_core.ValidationError):
            factory.register("", "test")
            factory.register(" ", "test")

    def test_functor_no_args(self):
        factory = Factory()
        factory.register("a", lambda: 1)
        self.assertEqual(factory.number_callbacks, 1)

    def test_functor_1_arg(self):
        factory = Factory()
        factory.register("a", lambda x: x**2)
        self.assertEqual(factory.number_callbacks, 1)

    def test_functor_3_args(self):
        factory = Factory()
        factory.register("a", lambda x, y, z: x+y+z)
        self.assertEqual(factory.number_callbacks, 1)

    def test_duplicated_key(self):
        factory = Factory()
        factory.register("a", lambda x: x**2)
        with self.assertRaises(KeyError):
            factory.register("a", lambda x, y: x+y)

    def test_remove_empty_key(self):
        factory = Factory()
        with self.assertRaises(pydantic_core._pydantic_core.ValidationError):
            factory.unregister("", "test")
            factory.unregister(" ", "test")

    def test_remove_non_existing_key(self):
        factory = Factory()
        factory.register("a", lambda x: x**2)
        with self.assertRaises(KeyError):
            factory.unregister("b")

    def test_remove_existing_key(self):
        factory = Factory()
        factory.register("a", lambda x: x**2)
        factory.unregister("a")
        self.assertEqual(factory.number_callbacks, 0)

    def test_create_empty_key(self):
        factory = Factory()
        with self.assertRaises(pydantic_core._pydantic_core.ValidationError):
            factory.create("", "test")
            factory.create(" ", "test")

    def test_create_non_existing_key(self):
        factory = Factory()
        factory.register("a", lambda x: x**2)
        with self.assertRaises(KeyError):
            factory.create("b")

    def test_create_existing_key(self):
        factory = Factory()
        factory.register("a", lambda x: x**2)
        factory.register("b", lambda x, y: x+y)
        factory.register("c", lambda x, y, z: x+y+z)
        self.assertEqual(factory.create("a", 2), 4)
        self.assertEqual(factory.create("b", 2, 4), 6)
        self.assertEqual(factory.create("c", 2, 4, z=10), 16)

    def test_invalid_create(self):
        factory = Factory()
        factory.register("a", lambda x: x**2)
        with self.assertRaises(TypeError):
            factory.create("a", 2, 4)
