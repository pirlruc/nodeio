"""The benchmark module contains decorators for benchmarking functions.

Decorators:
    timer - measures the execution time of a given function.
    memory - measures the memory usage and peak memory usage of a given
     function.
    timer_memory - measures the execution time and memory usage of a given
     function.
    benchmark - measures the average time and average memory consumption of a
     given function considering a number of repeats.
"""

import tracemalloc
from collections.abc import Callable
from functools import wraps
from time import perf_counter

from pydantic import validate_call
from pydantic.types import PositiveInt

from nodeio.infrastructure.logger import NodeIOLogger


@validate_call
def timer(functor: Callable):
    """Measures the time to execute a function."""

    @wraps(functor)
    def wrapper(*args, **kwargs):
        """
        This wrapper measures the execution time of a given function.

        It takes in a callable function as an argument and returns the result
         of that function. The decorator uses Python's built-in timer
         (`perf_counter`) to record the start and end times of the function's
         execution. It then logs the elapsed time and function name.

        Args:
            functor (Callable): The function to be timed.

        Returns:
            result: The result of the function being executed.
        """
        start_time = perf_counter()
        result = functor(*args, **kwargs)
        end_time = perf_counter()
        logger = NodeIOLogger().logger
        logger.info(
            "Function: %s -> Time Elapsed (s): %.6f",
            functor.__name__,
            end_time - start_time
        )
        logger.info("%s", "-" * 60)
        return result

    return wrapper


@validate_call
def memory(functor: Callable):
    """Measures the memory consumption of a function."""

    @wraps(functor)
    def wrapper(*args, **kwargs):
        """
        This wrapper measures the memory usage and peak memory usage of a
         given function.

        It takes in a callable function as an argument and returns the result
         of that function. The decorator uses Python's built-in `tracemalloc`
         to obtain the current and peak memory usage of a function. It then
         logs the memory usage, peak memory usage and function name.

        Args:
            functor (Callable): The function to be measured.

        Returns:
            result: The result of the function being executed.
        """
        tracemalloc.start()
        result = functor(*args, **kwargs)
        current, peak = tracemalloc.get_traced_memory()
        logger = NodeIOLogger().logger
        logger.info("Function: %s", functor.__name__)
        logger.info(
            "Memory Usage: %.6f MB \tPeak Memory Usage: %.6f MB ",
            current / 10**6,
            peak / 10**6
        )
        logger.info("%s", "-" * 60)
        tracemalloc.stop()
        return result

    return wrapper


@validate_call
def timer_memory(functor: Callable):
    """Measures the time and memory consumption of a function."""

    @wraps(functor)
    def wrapper(*args, **kwargs):
        """
        This wrapper measures both the execution time and memory usage
         of a given function.

        It takes in a callable function as an argument and returns the result
         of that function. The decorator uses Python's built-in timer
         (`perf_counter`) to record the start and end times of the function's
         execution, and it also uses `tracemalloc` to obtain the current and
         peak memory usage. It then logs the elapsed time, memory usage,
        and peak memory usage, along with the function name.

        Args:
            functor (Callable): The function to be timed and measured.

        Returns:
            result: The result of the function being executed.
        """
        tracemalloc.start()
        start_time = perf_counter()
        result = functor(*args, **kwargs)
        current, peak = tracemalloc.get_traced_memory()
        end_time = perf_counter()
        logger = NodeIOLogger().logger
        logger.info("Function: %s", functor.__name__)
        logger.info(
            "Memory Usage: %.6f MB \tPeak Memory Usage: %.6f MB ",
            current / 10**6,
            peak / 10**6
        )
        logger.info("Time Elapsed (s): %.6f", end_time - start_time)
        logger.info("%s", "-" * 60)
        tracemalloc.stop()
        return result

    return wrapper


@validate_call
def benchmark(_functor: Callable = None, *, number_repeats: PositiveInt = 100):
    """Benchmarks the time and memory consumption of a function."""

    def decorator_benchmark(functor):
        """
        The benchmark decorator is used to measure the time and memory 
        consumption of a given function. It takes in a callable function as an
        argument, repeats it the specified number of times (default is 100), 
        and logs the average elapsed time and peak memory usage for each run.
        """
        @wraps(functor)
        def wrapper(*args, **kwargs):
            """
            This decorator measures both the execution time and memory
             usage of a given function considering a number of repeats.

            It takes in a callable function as an argument and returns the
             result of that function. The decorator uses Python's built-in
             timer (`perf_counter`) to record the start and end times of the
             function's execution, and it also uses `tracemalloc` to obtain
             the current and peak memory usage. It then logs the average 
             elapsed time, average memory usage, and peak memory usage, along
             with the function name.

            Args:
                functor (Callable): The function to be timed and measured.

            Returns:
                result: The result of the function being executed.
            """
            tracemalloc.start()
            start_time = perf_counter()
            for _ in range(number_repeats):
                result = functor(*args, **kwargs)
            current, peak = tracemalloc.get_traced_memory()
            end_time = perf_counter()
            logger = NodeIOLogger().logger
            logger.info("Function: %s", functor.__name__)
            logger.info(
                "Memory Usage: %.6f MB \tPeak Memory Usage: %.6f MB ",
                current / 10**6,
                peak / 10**6
            )
            logger.info(
                "Time Elapsed (s): %.6f",
                (end_time - start_time)/number_repeats
            )
            logger.info("%s", "-" * 60)
            tracemalloc.stop()
            return result
        return wrapper

    if _functor is None:
        return decorator_benchmark
    return decorator_benchmark(_functor)
