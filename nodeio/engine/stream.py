from copy import deepcopy
from typing import Any, Optional

from pydantic import BaseModel, PrivateAttr, model_validator, validate_call
from typing_extensions import Self

import nodeio.engine.configuration
from nodeio.decorators.logging import log
from nodeio.engine.action import Action
from nodeio.engine.arguments import InputArg
from nodeio.infrastructure.constrained_types import key_str
from nodeio.infrastructure.logger import NodeIOLogger


class OutputStream(BaseModel, validate_assignment=True):
    """Output stream data."""

    key: key_str
    type: Optional[object] = None


class InputStream(BaseModel, validate_assignment=True):
    """Input stream data. This class associates an output stream with an input
    argument."""

    arg: InputArg
    stream: OutputStream
    actions: Optional[list[Action]] = list()

    @model_validator(mode="after")
    def __check_data_type_fields(self) -> Self:
        """Completes information regarding the input stream data and checks if
        data types are coherent.
        Only valid if there is no action to filter the output of an output
        stream.

        :return: Instance of InputStream with update
        :rtype: InputStream

        :raises TypeError: If the input argument and stream data types are not
        coherent.
        """
        if len(self.actions) == 0:
            if self.arg.type is None and self.stream.type is not None:
                self.arg.type = self.stream.type
            elif self.arg.type is not None and self.stream.type is None:
                self.stream.type = self.arg.type

            if (
                self.arg.type and self.stream.type
            ) is not None and self.arg.type != self.stream.type:
                error_message = f"Input argument type {self.arg.type} and "
                f"stream type {self.stream.type} are different. Please review "
                f"stream {self.stream.key}"
                NodeIOLogger().logger.error(error_message)
                raise TypeError(error_message)
        return self

    @staticmethod
    @validate_call
    @log
    def from_configuration(
        configuration: nodeio.engine.configuration.InputStream,
    ) -> Self:
        """Create InputStream instance from input stream configuration.

        :param configuration: Input stream configuration
        :type configuration: nodeio.engine.configuration.InputStream

        :return: InputStream instance
        :rtype: Self
        """
        actions = list()
        for action in configuration.actions:
            actions.append(Action.from_configuration(configuration=action))
        return InputStream(
            arg=InputArg(key=configuration.arg),
            stream=OutputStream(key=configuration.stream),
            actions=actions,
        )


class ContextStream(OutputStream):
    """Context stream data."""

    __value: Any = PrivateAttr(default=None)

    @validate_call
    @log
    def register(self, new_value: Any) -> Self:
        """Registers a new value in the output stream.

        :param new_value: New value obtained for the output stream
        :type new_value: Any

        :raises TypeError: New value data type does not correspond with the
        output stream data type.

        :return: Output stream with new value registered
        :rtype: OutputStream
        """
        # Do not try to infer data type from new_value at runtime. If the data
        # type is a Union the type will return only one of the values in the
        # Union which is not correct.
        if self.type is not None and not isinstance(new_value, self.type):
            error_message = f"Value type {type(new_value)} does not correspond"
            " to the type {self.type} registered for stream {self.key}."
            NodeIOLogger().logger.error(error_message)
            raise TypeError(error_message)
        self.__value = new_value
        return self

    @validate_call
    @log
    def get(self, actions: Optional[list[Action]] = list()) -> Any:
        """Obtains the value registered in the output stream. The value
        returned is a copy of the original value.

        :param actions: Actions to be performed to filter the value in the
        output stream
        :type actions: list[Action]

        :raises TypeError: Output stream data type does no correspond to the
        action data type.

        :return: Output stream filtered registered value
        :rtype: Any
        """
        result = self.__value
        for action in actions:
            if not isinstance(result, action.type):
                error_message = f"Type for selection {type(result)} does not "
                f"correspond to the type {action.type} registered for action "
                f"in stream {self.key}."
                NodeIOLogger().logger.error(error_message)
                raise TypeError(error_message)
            result = result[action.value]
        return deepcopy(result)

    @log
    def has_value(self) -> bool:
        """Check if there is a value registered for the stream.

        :return: If there is a value registered returns True. Otherwise returns
          False.
        :rtype: bool
        """
        return self.__value is not None

    @staticmethod
    @validate_call
    @log
    def from_output_stream(stream: OutputStream) -> Self:
        """Create a ContextStream instance from output stream.

        :param stream: Output stream
        :type stream: OutputStream

        :return: Context stream instance
        :rtype: Self
        """
        return ContextStream(key=stream.key, type=stream.type)
