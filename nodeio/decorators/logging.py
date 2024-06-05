from collections.abc import Callable
from functools import wraps

from pydantic import validate_call

from nodeio.infrastructure.constants import LOG_ENABLED
from nodeio.infrastructure.logger import NodeIOLogger


@validate_call
def log(functor: Callable):
    """Prints the function signature and return value."""

    @wraps(functor)
    def wrapper(*args, **kwargs):
        if LOG_ENABLED:
            args_repr = [repr(a) for a in args]
            kwargs_repr = [f"{k}={repr(v)}" for k, v in kwargs.items()]
            signature = ", ".join(args_repr + kwargs_repr)
            logger = NodeIOLogger().logger
            logger.debug("Function: %s", functor.__name__)
            logger.debug("Call: %s(%s)", functor.__name__, signature)
            result = functor(*args, **kwargs)
            logger.debug("Return: %s", repr(result))
            logger.debug("%s", "-" * 50)
        else:
            result = functor(*args, **kwargs)
        return result

    return wrapper
