import unittest

import nodeio.decorators.logging


@nodeio.decorators.logging.log
def add_one(value1, value2):
    return value1 + value2


@nodeio.decorators.logging.log
def add_two(value1, value2, value3):
    return value1 + value2 + value3


@nodeio.decorators.logging.log
def add_three(value1, value2, value3, value4):
    return value1 + value2 + value3 + value4


@nodeio.decorators.logging.log
def add_list(values: list):
    sum = 0
    for val in values:
        sum += val
    return sum


@nodeio.decorators.logging.log(enabled=False)
def add_list_not_enabled(values: list):
    sum = 0
    for val in values:
        sum += val
    return sum


class TestLoggingDecorators(unittest.TestCase):
    def test_debug_add_one(self):
        sum = add_one(1, value2=3)
        self.assertEqual(sum, 4)

    def test_debug_add_two(self):
        sum = add_two(1, value2=3, value3=4)
        self.assertEqual(sum, 8)

    def test_debug_add_three(self):
        sum = add_three(1, 3, value3=4, value4=6)
        self.assertEqual(sum, 14)

    def test_debug_add_list(self):
        sum = add_list([1, 3, 4, 6])
        self.assertEqual(sum, 14)

    def test_debug_add_list_not_enabled(self):
        sum = add_list_not_enabled([1, 3, 4, 6])
        self.assertEqual(sum, 14)
