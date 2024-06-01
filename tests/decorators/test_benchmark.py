import unittest
import time
import pydantic_core
import nodeio.decorators.benchmark

@nodeio.decorators.benchmark.timer
def add_one(value1, value2):
    time.sleep(0.5)
    return value1 + value2

@nodeio.decorators.benchmark.memory
def add_two(value1, value2, value3):
    time.sleep(0.5)
    return value1 + value2 + value3

@nodeio.decorators.benchmark.timer_memory
def add_three(value1, value2, value3, value4):
    time.sleep(0.5)
    return value1 + value2 + value3 + value4

@nodeio.decorators.benchmark.benchmark
def add_list(values: list):
    time.sleep(0.01)
    sum = 0
    for val in values:
        sum += val
    return sum

@nodeio.decorators.benchmark.benchmark(number_repeats=200)
def add_list_repeat(values: list):
    time.sleep(0.01)
    sum = 0
    for val in values:
        sum += val
    return sum

class TestBenchmarkDecorators(unittest.TestCase):
    def test_timer(self):
        sum = add_one(1,value2=3)
        self.assertEqual(sum,4)

    def test_memory(self):
        sum = add_two(1,value2=3, value3=4)
        self.assertEqual(sum,8)

    def test_timer_memory(self):
        sum = add_three(1,3, value3=4, value4=6)
        self.assertEqual(sum,14)

    def test_benchmark_100(self):
        sum = add_list([1,3,4,6])
        self.assertEqual(sum,14)

    def test_benchmark_repeat_200(self):
        sum = add_list_repeat([1,3,4,6])
        self.assertEqual(sum,14)

    def test_benchmark_repeat_float(self):
        with self.assertRaises(pydantic_core._pydantic_core.ValidationError):
            @nodeio.decorators.benchmark.benchmark(number_repeats=1.4)
            def invalid_repeat(values: list):
                pass

    def test_benchmark_repeat_zero(self):
        with self.assertRaises(pydantic_core._pydantic_core.ValidationError):
            @nodeio.decorators.benchmark.benchmark(number_repeats=0)
            def invalid_repeat(values: list):
                pass

