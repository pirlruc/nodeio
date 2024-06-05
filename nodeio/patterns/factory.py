from collections.abc import Callable
from typing import Any

from pydantic import BaseModel, PrivateAttr, validate_call
from typing_extensions import Self

from nodeio.decorators.logging import log
from nodeio.infrastructure.constrained_types import KeyStr
from nodeio.infrastructure.logger import NodeIOLogger


class Factory(BaseModel, validate_assignment=True):
    """Factory pattern implementation for a class."""

    __callbacks: dict[Callable] = PrivateAttr(default={})

    @property
    def number_callbacks(self) -> int:
        """Returns the number of callbacks registered in factory.

        :return: Number of callbacks
        :rtype: int
        """
        return len(self.__callbacks)

    @validate_call
    @log
    def register(self, key: KeyStr, functor: Callable) -> Self:
        """Registers a functor with a given key.

        :param key: Key identifying the functor
        :type key: str
        :param functor: Functor.
        :type functor: Callable

        :return: Factory object with the new functor registered.
        :rtype: Factory

        :raises KeyError: If the key is already defined in factory.
        """
        if key in self.__callbacks.keys():
            error_message = "Functor key already defined in factory"
            NodeIOLogger().logger.error(error_message)
            raise KeyError(error_message)
        self.__callbacks[key] = functor
        return self

    @validate_call
    @log
    def unregister(self, key: KeyStr) -> Self:
        """Unregisters the functor associated with a given key.

        :param key: Key identifying the functor
        :type key: str

        :return: Factory object with the specified creation functor removed.
        :rtype: Factory

        :raises KeyError: If the key is not present in factory.
        """
        if key not in self.__callbacks.keys():
            error_message = "Functor key not present in factory"
            NodeIOLogger().logger.error(error_message)
            raise KeyError(error_message)
        self.__callbacks.pop(key, None)
        return self

    @validate_call
    @log
    def create(self, key: KeyStr, *args, **kwargs) -> Any:
        """Returns the result of the functor registered with the provided key.

        :param key: Key identifying the functor
        :type key: str

        :return: Result of the functor.

        :raises KeyError: If the key is not present in factory.
        """
        if key not in self.__callbacks.keys():
            error_message = "Functor key not present in factory"
            NodeIOLogger().logger.error(error_message)
            raise KeyError(error_message)
        return self.__callbacks[key](*args, **kwargs)
