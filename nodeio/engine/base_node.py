"""The base node module define the BaseNode abstract class to define all nodes.

Classes:
    BaseNode - abstract class to define all nodes.
"""

from abc import ABC, abstractmethod
from typing import Any, Optional

from pydantic import validate_call
from typing_extensions import Self

from nodeio.decorators.logging import log
from nodeio.infrastructure.constrained_types import KeyStr


class BaseNode(ABC):
    """BaseNode is a base class for nodes within a graph."""

    __name: KeyStr

    @property
    def name(self) -> KeyStr:
        """Obtains the node name identifier.

        :return: Node name identifier
        :rtype: KeyStr
        """
        return self.__name

    @name.setter
    @validate_call
    def name(self, new_name: KeyStr):
        """Updates the node name identifier.

        :param new_name: Node name identifier
        :type new_name: KeyStr
        """
        self.__name = new_name

    @validate_call
    @log
    def __init__(self, name: Optional[KeyStr] = None):
        """Creates a node instance."""
        self.__name = self.__class__.__name__.lower()
        if name is not None:
            self.__name = name

    @abstractmethod
    def load(self, *args, **kwargs) -> Self:
        """Configures a node."""

    @abstractmethod
    def process(self, *args, **kwargs) -> Any:
        """Performs the main computation of a node. This method is called by
        the framework whenever the inputs for the method are available."""
