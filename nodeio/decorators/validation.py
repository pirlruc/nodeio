"""The validation module contains decorators for validating functions.

Decorators:
    custom_validate_call - enables or disables pydantic validate_call.
"""

from functools import wraps
from typing import Callable

from pydantic import validate_call


@validate_call
def custom_validate_call(_functor: Callable = None, *, enabled: bool = True):
    """Decorator for turning on and off the pydantic validate_call."""
    @wraps(_functor)
    def wrapper(functor):
        """Wrapper that enables or disables the validate_call method."""
        if enabled:
            return validate_call(functor)
        return functor

    if _functor is None:
        return wrapper
    return wrapper(_functor)
