"""The arguments module define classes that describe the inputs and outputs of
 a function or method based on their annotations.

Classes:
    InputArg - describes an input argument.
    OutputArg - describes an output argument.
"""

from inspect import Parameter
from typing import Annotated, Optional

from pydantic import BaseModel, ConfigDict, model_validator, validate_call
from pydantic.functional_validators import AfterValidator
from typing_extensions import Self

from nodeio.decorators.logging import log
from nodeio.infrastructure.constants import LOGGING_ENABLED
from nodeio.infrastructure.constrained_types import KeyStr
from nodeio.infrastructure.logger import NodeIOLogger


# TODO: Remove validate_call
@validate_call
@log(enabled=LOGGING_ENABLED)
def __transform_parameter_empty_to_none(value: object):
    """Converts an empty parameter annotation to None.

    :param value: Parameter annotation
    :type value: object
    :return: Returns None if the parameter annotation is empty, otherwise
     returns the annotation itself.
    :rtype: object
    """
    if value is Parameter.empty:
        return None
    return value


_InspectTransformedObject = Annotated[
    object, AfterValidator(__transform_parameter_empty_to_none)
]


class InputArg(BaseModel, validate_assignment=True):
    """Information regarding input arguments."""

    key: KeyStr
    type: Optional[_InspectTransformedObject] = None
    default: Optional[_InspectTransformedObject] = None

    @model_validator(mode="after")
    @log(enabled=LOGGING_ENABLED)
    def _check_type_default_fields(self) -> Self:
        """Completes information regarding the input argument and checks if the
        default and the data type are coherent.

        :return: Instance of InputArg with update
        :rtype: Self

        :raises TypeError: If the default value and the data type are not
        coherent.
        """
        if self.default is not None and self.type is None:
            self.type = type(self.default)

        if (self.default and self.type) is not None and not isinstance(
            self.default, self.type
        ):
            error_message = \
                f"Default value {self.default} does not agree with " \
                f"type {self.type}. Please review argument annotation " \
                f"for {self.key}"
            NodeIOLogger().logger.error(error_message)
            raise TypeError(error_message)
        return self

    @staticmethod
    @validate_call(config=ConfigDict(arbitrary_types_allowed=True))
    @log(enabled=LOGGING_ENABLED)
    def from_parameter(parameter: Parameter) -> Self:
        """Creates an InputArg from inspect parameter.

        :param parameter: Parameter annotation from inspect.signature
        :type parameter: inspect.Parameter

        :return: Input argument information
        :rtype: Self
        """
        return InputArg(
            key=parameter.name,
            type=parameter.annotation,
            default=parameter.default,
        )


class OutputArg(BaseModel, validate_assignment=True):
    """Information regarding output arguments."""

    type: Optional[_InspectTransformedObject] = None

    @staticmethod
    @validate_call
    @log(enabled=LOGGING_ENABLED)
    def from_return_annotation(parameter: object) -> Self:
        """Creates an OutputArg from inspect return annotation.

        :param parameter: Return annotation from inspect.signature
        :type parameter: object

        :return: Output argument information
        :rtype: Self
        """
        return OutputArg(type=parameter)
