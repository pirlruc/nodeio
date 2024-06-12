"""The node handler module contains the objects needed to manage nodes in
 a graph.

Classes:
    NodeHandler - manages the interface with a callable function or method
     via configuration using input and output streams. The class provides a
     synchronous and an assynchronous interface to the callable.
"""

from inspect import isabstract, signature
from typing import Callable, Optional

from pydantic import BaseModel, Field, PrivateAttr, validate_call
from typing_extensions import Self

from nodeio.decorators.logging import log
from nodeio.engine.arguments import InputArg, OutputArg
from nodeio.engine.configuration import Node
from nodeio.engine.stream import ContextStream, InputStream, OutputStream
from nodeio.engine.stream_handler import StreamHandler
from nodeio.infrastructure.constrained_types import KeyStr
from nodeio.infrastructure.exceptions import ConfigurationError
from nodeio.infrastructure.logger import NodeIOLogger


class NodeHandler(BaseModel, validate_assignment=True):
    """Handle node processing instances."""

    name: KeyStr = Field(frozen=True)
    functor: Callable = Field(frozen=True)
    __inputs: Optional[dict[KeyStr, InputArg]] = PrivateAttr(default={})
    __output: Optional[OutputArg] = PrivateAttr(default=OutputArg())
    __input_streams: Optional[list[InputStream]] = PrivateAttr(default=[])
    __output_stream: Optional[OutputStream] = PrivateAttr(default=None)

    @property
    def number_inputs(self) -> int:
        """Returns the number of inputs associated with the functor.

        :return: Number of inputs
        :rtype: int
        """
        return len(self.__inputs)

    @property
    def number_input_streams(self) -> int:
        """Returns the number of input streams registered in handler.

        :return: Number of input streams
        :rtype: int
        """
        return len(self.__input_streams)

    @validate_call
    @log
    def __init__(self, name: KeyStr, functor: Callable, **data) -> Self:
        """Creates a NodeHandler instance base on a name identifier and a
         functor.

        :param name: Node name identifier
        :type name: KeyStr
        :param functor: Processing functor
        :type functor: Callable

        :raises TypeError: If functor returns an abstract class.

        :return: Node handler instance
        :rtype: Self
        """
        properties = signature(functor)
        if isabstract(properties.return_annotation):
            error_message = "Functor return annotation provided an " \
                f"incorrect type for node {name}. Please review functor " \
                "annotation"
            NodeIOLogger().logger.error(error_message)
            raise TypeError(error_message)

        super().__init__(name=name, functor=functor, **data)
        inputs = properties.parameters
        for key in inputs.keys():
            self.__inputs[key] = InputArg.from_parameter(parameter=inputs[key])
        self.__output = OutputArg.from_return_annotation(
            parameter=properties.return_annotation
        )

    @validate_call
    @log
    def load(self, stream_handler: StreamHandler, configuration: Node) -> Self:
        """Loads the configuration for the node processing handler.

        :param stream_handler: Handler for input and output streams.
        :type stream_handler: StreamHandler
        :param configuration: Configuration for the node.
        :type configuration: Node

        :raises RuntimeError: If node name identifier (configuration) does not
         match with the node handler name identifier (self).
        :raises ConfigurationError: If functor has mandatory inputs but there
         are no input streams registered in the handler.
        :raises ConfigurationError: If input argument associated with an input
         stream does not exist in the functor registered in the handler.
        :raises ConfigurationError: If there is no output stream associated
         with an input stream key.

        :return: Updated node handler instance.
        :rtype: Self
        """
        if self.name != configuration.node:
            error_message = f"Node identifier {configuration.node} does not " \
                f"match handler identifier {self.name}"
            NodeIOLogger().logger.error(error_message)
            raise RuntimeError(error_message)

        if (
            len(configuration.input_streams) == 0
            and self.__has_mandatory_inputs()
        ):
            error_message = "Functor has mandatory input arguments for node " \
                f"{self.name}. Please review configuration to add input " \
                "streams"
            NodeIOLogger().logger.error(error_message)
            raise ConfigurationError(error_message)

        for input_stream_config in configuration.input_streams:
            if input_stream_config.arg not in self.__inputs.keys():
                error_message = f"Input arg {input_stream_config.arg} " \
                    f"provided for stream {input_stream_config.stream} does " \
                    "not exist in functor. Please review configuration"
                NodeIOLogger().logger.error(error_message)
                raise ConfigurationError(error_message)

            try:
                output_stream = stream_handler.get_output_stream(
                    key=input_stream_config.stream
                )
            except KeyError as error:
                error_message = f"Input stream {input_stream_config.stream} " \
                    f"defined for node {self.name} does not have an output " \
                    "stream associated. Please review configuration"
                NodeIOLogger().logger.error(error_message)
                raise ConfigurationError(error_message) from error

            input_stream: InputStream = InputStream.from_configuration(
                configuration=input_stream_config
            )
            input_stream.stream = output_stream
            input_stream.arg = self.__inputs[input_stream_config.arg]
            self.__input_streams.append(input_stream)
            stream_handler.register_connection(
                key=input_stream_config.stream, ending=self.name
            )

        if configuration.output_stream is not None:
            self.__output_stream = OutputStream(
                key=configuration.output_stream, type=self.__output.type
            )
            stream_handler.add_output_stream(
                stream=self.__output_stream, origin=self.name
            )
        return self

    @validate_call
    def process(
        self, context: Optional[dict[KeyStr, ContextStream]] = None
    ) -> dict[KeyStr, ContextStream]:
        """Process node registered in handler synchronously.

        :param context: Current processing context, defaults to dict()
        :type context: Optional[dict[KeyStr, ContextStream]], optional

        :return: Updated processing context
        :rtype: dict[KeyStr, ContextStream]
        """
        if context is None:
            context = {}

        input_context = {}
        for input_stream in self.__input_streams:
            input_context[input_stream.arg.key] = \
                context[input_stream.stream.key].get(
                    actions=input_stream.actions
            )
        result = self.functor(**input_context)
        if self.__output_stream is not None:
            context_stream: ContextStream = ContextStream.from_output_stream(
                self.__output_stream
            )
            context_stream.register(new_value=result)
            context[context_stream.key] = context_stream
        return context

    async def process_async(
        self, context: Optional[dict[KeyStr, ContextStream]] = None
    ) -> dict[KeyStr, ContextStream]:
        """Process node registered in handler asynchronously.

        :param context: Current processing context, defaults to dict()
        :type context: Optional[dict[KeyStr, ContextStream]], optional

        :return: Updated processing context
        :rtype: dict[KeyStr, ContextStream]
        """
        if context is None:
            context = {}
        return self.process(context=context)

    @log
    def has_output(self) -> bool:
        """Checks if there is an output annotation for the functor.

        :return: Returns true if functor has a return annotation, and false
        otherwise.
        :rtype: bool
        """
        return self.__output.type is not None

    @log
    def has_output_stream(self) -> bool:
        """Checks if there is an output stream registered in handler.

        :return: Returns true if handler has an output stream, and false
        otherwise.
        :rtype: bool
        """
        return self.__output_stream is not None

    @log
    def __has_mandatory_inputs(self) -> bool:
        """Checks if there are mandatory inputs for the functor in the handler.

        :return: Returns true if there are mandatory inputs, and false
        otherwise.
        :rtype: bool
        """
        return any(input.default is None for _, input in self.__inputs.items())
