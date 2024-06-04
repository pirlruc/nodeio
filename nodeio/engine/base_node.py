from abc import ABC, abstractmethod
from typing import Any, Optional

from pydantic import validate_call
from typing_extensions import Self

from nodeio.decorators.logging import log
from nodeio.infrastructure.constrained_types import key_str


class BaseNode(ABC):
    """BaseNode is a base class for nodes within a graph."""

    __id: key_str

    @property
    def id(self) -> key_str:
        """Obtains the node identifier.

        :return: Node identifier
        :rtype: key_str
        """
        return self.__id

    @id.setter
    @validate_call
    def id(self, new_id: key_str):
        """Updates the node identifier.

        :param new_id: Node identifier
        :type new_id: key_str
        """
        self.__id = new_id

    @validate_call
    @log
    def __init__(self, id: Optional[key_str] = None):
        """Creates a node instance."""
        self.__id = self.__class__.__name__.lower()
        if id is not None:
            self.id = id

    @abstractmethod
    def load(self, *args, **kwargs) -> Self:
        """Configures a node."""
        pass

    @abstractmethod
    def process(self, *args, **kwargs) -> Any:
        """Performs the main computation of a node. This method is called by
        the framework whenever the inputs for the method are available."""
        pass
