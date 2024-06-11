"""The singleton module contains decorators for implementing the singleton
 pattern.

Decorators:
    singleton - allows to create singleton classes.
"""

import functools
from threading import Lock


def singleton(cls):
    """Make a class a singleton class (with only one instance)."""

    @functools.wraps(cls)
    def wrapper(*args, **kwargs):
        """
        This decorator transforms the given class into a singleton class,
         ensuring that only one instance of the class is ever created across
         all threads and process calls.

        The `__lock` attribute in the wrapper function ensures thread safety
         by synchronizing access to the singleton instance.
        """
        with wrapper.__lock:
            if wrapper.__instance is None:
                wrapper.__instance = cls(*args, **kwargs)
        return wrapper.__instance

    wrapper.__instance = None
    wrapper.__lock = Lock()
    return wrapper
