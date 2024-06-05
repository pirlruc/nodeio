import functools
from threading import Lock


def singleton(cls):
    """Make a class a singleton class (with only one instance)."""

    @functools.wraps(cls)
    def wrapper(*args, **kwargs):
        with wrapper.__lock:
            if wrapper.__instance is None:
                wrapper.__instance = cls(*args, **kwargs)
        return wrapper.__instance

    wrapper.__instance = None
    wrapper.__lock = Lock()
    return wrapper
