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
        start_time = perf_counter()
        result = functor(*args, **kwargs)
        end_time = perf_counter()
        logger = NodeIOLogger().logger
        logger.info(
            f" Function: {functor.__name__} -> Time Elapsed (s): "
            f"{end_time - start_time:.6f}"
        )
        logger.info(f' {"-" * 50}')
        return result

    return wrapper


@validate_call
def memory(functor: Callable):
    """Measures the memory consumption of a function."""

    @wraps(functor)
    def wrapper(*args, **kwargs):
        tracemalloc.start()
        result = functor(*args, **kwargs)
        current, peak = tracemalloc.get_traced_memory()
        logger = NodeIOLogger().logger
        logger.info(f" Function: {functor.__name__}")
        logger.info(
            f" Memory Usage: {current / 10**6:.6f} MB \t"
            f" Peak Memory Usage: {peak / 10**6:.6f} MB "
        )
        logger.info(f' {"-" * 50}')
        tracemalloc.stop()
        return result

    return wrapper


@validate_call
def timer_memory(functor: Callable):
    """Measures the time and memory consumption of a function."""

    @wraps(functor)
    def wrapper(*args, **kwargs):
        tracemalloc.start()
        start_time = perf_counter()
        result = functor(*args, **kwargs)
        current, peak = tracemalloc.get_traced_memory()
        end_time = perf_counter()
        logger = NodeIOLogger().logger
        logger.info(f" Function: {functor.__name__}")
        logger.info(
            f" Memory Usage: {current / 10**6:.6f} MB \t"
            f" Peak Memory Usage: {peak / 10**6:.6f} MB "
        )
        logger.info(f" Time Elapsed (s): {end_time - start_time:.6f}")
        logger.info(f' {"-" * 50}')
        tracemalloc.stop()
        return result

    return wrapper


@validate_call
def benchmark(_functor: Callable = None, *, number_repeats: PositiveInt = 100):
    """Benchmarks the time and mmemory consumption of a function."""

    def decorator_benchmark(functor):
        @wraps(functor)
        def wrapper(*args, **kwargs):
            tracemalloc.start()
            start_time = perf_counter()
            for _ in range(number_repeats):
                result = functor(*args, **kwargs)
            current, peak = tracemalloc.get_traced_memory()
            end_time = perf_counter()
            logger = NodeIOLogger().logger
            logger.info(f" Function: {functor.__name__}")
            logger.info(
                f" Memory Usage: {current / 10**6:.6f} MB \t"
                f" Peak Memory Usage: {peak / 10**6:.6f} MB "
            )
            logger.info(
                " Time Elapsed (s): "
                f"{(end_time - start_time)/number_repeats:.6f}"
            )
            logger.info(f' {"-" * 50}')
            tracemalloc.stop()
            return result

        return wrapper

    if _functor is None:
        return decorator_benchmark
    return decorator_benchmark(_functor)
