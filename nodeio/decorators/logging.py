from pydantic import validate_call
from collections.abc import Callable
from functools import wraps
from nodeio.infrastructure.logger import NodeIOLogger
from nodeio.infrastructure.constants import LOG_ENABLED

@validate_call
def log(functor: Callable):
    """Prints the function signature and return value"""

    @wraps(functor)
    def wrapper(*args, **kwargs):
        if LOG_ENABLED:
            args_repr = [repr(a) for a in args]
            kwargs_repr = [f"{k}={repr(v)}" for k, v in kwargs.items()]
            signature = ", ".join(args_repr + kwargs_repr)
            logger = NodeIOLogger().logger
            logger.debug(f' Function: {functor.__name__}')
            logger.debug(f' Call: {functor.__name__}({signature})')
            result = functor(*args, **kwargs)
            logger.debug(f' Return: {repr(result)}')
            logger.debug(f' {"-" * 50}')
        else:
            result = functor(*args, **kwargs)
        return result
    return wrapper
