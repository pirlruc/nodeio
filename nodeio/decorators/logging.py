"""The logging module contains decorators for logging functions.

Decorators:
    log - registers the function signature and return value.
"""

from collections.abc import Callable
from functools import wraps

from pydantic import validate_call

from nodeio.infrastructure.logger import NodeIOLogger


@validate_call
def log(_functor: Callable = None, *, enabled: bool = True):
    """Prints the function signature and return value."""

    def decorator_log(functor):
        """
        The log decorator registers the function signature and return value
         when called.
        """
        @wraps(functor)
        def wrapper(*args, **kwargs):
            """
            This wrapper prints the function signature and return value. This
             decorator logs the function name, its arguments, and its return
             value if logging is enabled.

            Args:
                functor (Callable): The function to wrap.
                enabled (bool): Whether to enable or disable logging.
                 Default is True.

            Returns:
                The result of the wrapped function.
            """
            if enabled:
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

    if _functor is None:
        return decorator_log
    return decorator_log(_functor)
