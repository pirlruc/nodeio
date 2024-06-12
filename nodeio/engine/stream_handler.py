"""The stream handler module contains the objects needed to manage streams.

Classes:
    StreamHandler - manages the streams generated by the nodes in a graph and
     the connections between nodes through the output and input streams.
"""

from networkx import DiGraph
from pydantic import BaseModel, Field, PrivateAttr, validate_call
from typing_extensions import Self

from nodeio.decorators.logging import log
from nodeio.engine.stream import OutputStream
from nodeio.infrastructure.constrained_types import KeyStr
from nodeio.infrastructure.exceptions import ConfigurationError
from nodeio.infrastructure.logger import NodeIOLogger


class _Connection(BaseModel):
    """Identifies the node that originate an output stream and defines if
    the output stream is connected to an input stream."""
    origin: KeyStr
    stream: OutputStream
    connected: bool = False


class StreamHandler(
    BaseModel, validate_assignment=True, arbitrary_types_allowed=True
):
    """Handle input and output stream connections."""

    graph: DiGraph = Field(frozen=True)
    __output: dict[KeyStr, _Connection] = PrivateAttr(default={})

    @property
    def number_output_streams(self) -> int:
        """Returns the number of output streams registered in handler.

        :return: Number of output streams
        :rtype: int
        """
        return len(self.__output)

    @validate_call
    @log
    def add_output_stream(self, stream: OutputStream, origin: KeyStr) -> Self:
        """Add output stream to stream handler.

        :param stream: Output stream to add.
        :type stream: OutputStream
        :param stream: Key of the node that originates the output stream.
        :type stream: KeyStr

        :raises KeyError: Output stream already exists.

        :return: Instance updated with the output stream
        :rtype: Self
        """
        if stream.key in self.__output.keys():
            error_message = f"Output stream {stream.key} already exists. " \
                "Please review stream key"
            NodeIOLogger().logger.error(error_message)
            raise KeyError(error_message)
        self.__output[stream.key] = _Connection(
            origin=origin, stream=stream
        )
        return self

    @validate_call
    @log
    def get_output_stream(self, key: KeyStr) -> OutputStream:
        """Obtain output stream registered.

        :param key: Key that identifies the output stream
        :type key: KeyStr

        :raises KeyError: Output stream key not registered.

        :return: Output stream associated with key
        :rtype: OutputStream
        """
        if key not in self.__output.keys():
            error_message = f"Output stream {key} does not exist. Please " \
                "review configuration"
            NodeIOLogger().logger.error(error_message)
            raise KeyError(error_message)
        return self.__output[key].stream

    @validate_call
    @log
    def register_connection(self, key: KeyStr, ending: KeyStr) -> Self:
        """Register a connection between an input and output stream.

        :param key: Key of the output stream to register the connection with
        an input stream.
        :type key: KeyStr
        :param key: Key of the node that is registering the connection
        :type key: KeyStr

        :raises KeyError: Output stream key does not exist.
        :raises ConfigurationError: Origin and ending node for stream is
         the same.

        :return: Instance updated with the output stream connection.
        :rtype: Self
        """
        if key not in self.__output.keys():
            error_message = f"Output stream {key} does not exist. Please " \
                "review configuration"
            NodeIOLogger().logger.error(error_message)
            raise KeyError(error_message)

        origin_node = self.__output[key].origin
        if ending == origin_node:
            error_message = f"Origin node {origin_node} and ending node " \
                f"{ending} for stream {key} is the same. Please review " \
                "configuration"
            NodeIOLogger().logger.error(error_message)
            raise ConfigurationError(error_message)

        self.graph.add_edge(u_of_edge=origin_node, v_of_edge=ending)
        self.__output[key].connected = True
        return self

    @log
    def has_unconnected_streams(self) -> bool:
        """Checks if there are any unconnected output streams.

        :return: Returns true if there any unconnected output streams. If all
        output streams are connected, returns false.
        :rtype: bool
        """
        return any(
            stream.connected is False for _, stream in self.__output.items()
        )

    @log
    def get_unconnected_stream_keys(self) -> list[KeyStr]:
        """Obtain unconnected stream keys.

        :return: Returns unconnected output streams.
        :rtype: list[KeyStr]
        """
        return [
            key
            for key, stream in self.__output.items()
            if not stream.connected
        ]
