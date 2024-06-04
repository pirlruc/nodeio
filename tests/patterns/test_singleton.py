import unittest
from threading import Thread

from nodeio.patterns.singleton import singleton


@singleton
class TestOne:
    pass


@singleton
class TestTwo:
    pass


@singleton
class TestThread:
    value: str = None

    def __init__(self, value: str) -> None:
        self.value = value


def print_value_thread(value: str, result: dict) -> None:
    singleton = TestThread(value)
    result[value] = {"value": singleton.value, "id": id(singleton)}


class TestSingleton(unittest.TestCase):
    def test_singleton_creation(self):
        self.assertEqual(id(TestOne()), id(TestOne()))
        self.assertEqual(id(TestTwo()), id(TestTwo()))
        self.assertNotEqual(id(TestOne()), id(TestTwo()))

    def test_singleton_thread(self):
        results = dict()
        process1 = Thread(target=print_value_thread, args=("FOO", results))
        process2 = Thread(target=print_value_thread, args=("BAR", results))
        process1.start()
        process2.start()
        process1.join()
        process2.join()
        self.assertEqual(results["FOO"], results["BAR"])
