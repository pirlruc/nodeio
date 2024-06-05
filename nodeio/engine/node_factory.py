from collections.abc import Callable
from inspect import isabstract, isclass, signature

from pydantic import validate_call
from typing_extensions import Self

from nodeio.decorators.logging import log
from nodeio.engine.base_node import BaseNode
from nodeio.infrastructure.constrained_types import KeyStr
from nodeio.infrastructure.logger import NodeIOLogger
from nodeio.patterns.factory import Factory


class NodeFactory(Factory):
    """Factory for creating processing nodes."""

    @validate_call
    @log
    def register(self, key: KeyStr, functor: Callable) -> Self:
        """Registers a node creation functor with a given key.

        :param key: Key identifying the node creation functor
        :type key: str
        :param functor: Node creation functor.
        :type functor: Callable

        :return: Factory object with the new node creation functor registered.
        :rtype: NodeFactory

        :raises TypeError: If the return annotation associated with the functor
          is not a BaseNode
        """
        # Although this validation is performed there is no guarantee that the
        # create method will return a BaseNode.
        # This is only guaranteed in the create method.
        return_values = signature(functor).return_annotation
        if return_values != Self and (
            not isclass(return_values)
            or isabstract(return_values)
            or not isinstance(return_values(), BaseNode)
        ):
            error_message = "Functor return annotation provided an " \
                f"incorrect type for key {key}. Please review " \
                "functor annotation"
            NodeIOLogger().logger.error(error_message)
            raise TypeError(error_message)
        super().register(key, functor)
        return self

    @validate_call
    @log
    def create(self, key: KeyStr, *args, **kwargs) -> BaseNode:
        """Returns a node created by the functor registered with the provided
        key.

        :param key: Key identifying the node creation functor
        :type key: str

        :return: Node created by the functor.
        :rtype: BaseNode

        :raises TypeError: If the node created by the functor is not a
        BaseNode.
        """
        result = super().create(key, *args, **kwargs)
        if not isinstance(result, BaseNode):
            error_message = f"Functor for key {key} returns an incorrect " \
                f"type: {type(result)}. Please review functor"
            NodeIOLogger().logger.error(error_message)
            raise TypeError(error_message)
        return result
