import unittest

import pydantic_core

import nodeio.engine.configuration
from nodeio.engine.action import Action, DictAction, ListAction


class ActionA(Action):
    pass


class TestAction(unittest.TestCase):
    def test_invalid_constructor(self):
        with self.assertRaises(pydantic_core.ValidationError):
            ActionA(type=list)
            ActionA(type=list, value="")
            ActionA(type=list, value="  ")
            ActionA(type=1, value=1)

    def test_invalid_update(self):
        action = ActionA(value=1)
        with self.assertRaises(pydantic_core.ValidationError):
            action.value = ""
            action.value = "  "

    def test_constructor(self):
        action = ActionA(value=1)
        self.assertIsNone(action.type)
        self.assertEqual(action.value, 1)

    def test_update(self):
        action = ActionA(value=1)
        action.value = "a"
        self.assertIsNone(action.type)
        self.assertEqual(action.value, "a")

    def test_from_list_configuration(self):
        config = nodeio.engine.configuration.ListAction(index=1)
        action = Action.from_configuration(configuration=config)
        self.assertEqual(action.type, list)
        self.assertEqual(action.value, 1)

    def test_from_dict_configuration(self):
        config = nodeio.engine.configuration.DictAction(key="a")
        action = Action.from_configuration(configuration=config)
        self.assertEqual(action.type, dict)
        self.assertEqual(action.value, "a")


class TestListAction(unittest.TestCase):
    def test_invalid_constructor(self):
        with self.assertRaises(pydantic_core.ValidationError):
            ListAction(value="a")

    def test_constructor(self):
        action = ListAction(value=1)
        self.assertEqual(action.type, list)
        self.assertEqual(action.value, 1)

    def test_invalid_update(self):
        action = ListAction(value=1)
        with self.assertRaises(pydantic_core.ValidationError):
            action.value = "a"

    def test_update(self):
        action = ListAction(value=1)
        action.value = 2
        self.assertEqual(action.type, list)
        self.assertEqual(action.value, 2)

    def test_from_configuration(self):
        config = nodeio.engine.configuration.ListAction(index=1)
        action = ListAction.from_configuration(configuration=config)
        self.assertEqual(action.type, list)
        self.assertEqual(action.value, 1)


class TestDictAction(unittest.TestCase):
    def test_invalid_constructor(self):
        with self.assertRaises(pydantic_core.ValidationError):
            DictAction(value="")
            DictAction(value="  ")
            DictAction(value=1)

    def test_constructor(self):
        action = DictAction(value="a")
        self.assertEqual(action.type, dict)
        self.assertEqual(action.value, "a")

    def test_invalid_update(self):
        action = DictAction(value="a")
        with self.assertRaises(pydantic_core.ValidationError):
            action.value = 1

    def test_update(self):
        action = DictAction(value="a")
        action.value = "b"
        self.assertEqual(action.type, dict)
        self.assertEqual(action.value, "b")

    def test_from_configuration(self):
        config = nodeio.engine.configuration.DictAction(key="a")
        action = DictAction.from_configuration(configuration=config)
        self.assertEqual(action.type, dict)
        self.assertEqual(action.value, "a")
