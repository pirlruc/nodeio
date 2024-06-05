from abc import ABC
from typing import Union

from pydantic import BaseModel, PrivateAttr, validate_call
from typing_extensions import Self

import nodeio.engine.configuration
from nodeio.decorators.logging import log
from nodeio.infrastructure.constrained_types import KeyStr
from nodeio.infrastructure.logger import NodeIOLogger


class Action(BaseModel, ABC, validate_assignment=True):
    """Abstract model for defining actions over data types."""

    _type: object = PrivateAttr(default=None)
    value: Union[KeyStr, int]

    @property
    def type(self) -> object:
        """Returns the data type associated with the action.

        :return: Data type associated with the action
        :rtype: object
        """
        return self._type

    @staticmethod
    @validate_call
    @log
    def from_configuration(
        configuration: Union[
            nodeio.engine.configuration.ListAction,
            nodeio.engine.configuration.DictAction,
        ],
    ) -> Self:
        """Create Action instance from configuration.

        :param configuration: List or dict action configuration
        :type configuration: Union[
            nodeio.engine.configuration.ListAction,
            nodeio.engine.configuration.DictAction
            ]

        :raises NotImplementedError: If action type is not implemented

        :return: Action instance
        :rtype: Self
        """
        if isinstance(configuration, nodeio.engine.configuration.ListAction):
            return ListAction.from_configuration(configuration=configuration)
        if isinstance(configuration, nodeio.engine.configuration.DictAction):
            return DictAction.from_configuration(configuration=configuration)
        error_message = (
            f"Action type {type(configuration)} not implemented"
        )
        NodeIOLogger().logger.error(error_message)
        raise NotImplementedError(error_message)


class ListAction(Action):
    """Actions for lists: 1) selection by index."""

    _type = list
    value: int

    @staticmethod
    @validate_call
    @log
    def from_configuration(
        configuration: nodeio.engine.configuration.ListAction,
    ) -> Self:
        """Create ListAction instance from list action configuration.

        :param configuration: List action configuration
        :type configuration: nodeio.engine.configuration.ListAction

        :return: ListAction instance
        :rtype: Self
        """
        return ListAction(value=configuration.index)


class DictAction(Action):
    """Actions for dictionaries: 1) selection by keyword."""

    _type = dict
    value: KeyStr

    @staticmethod
    @validate_call
    @log
    def from_configuration(
        configuration: nodeio.engine.configuration.DictAction,
    ) -> Self:
        """Create DictAction instance from dict action configuration.

        :param configuration: Dict action configuration
        :type configuration: nodeio.engine.configuration.DictAction

        :return: DictAction instance
        :rtype: Self
        """
        return DictAction(value=configuration.key)
