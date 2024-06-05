from typing import Annotated, Literal, Optional, Union

from pydantic import BaseModel, Field, model_validator
from typing_extensions import Self

from nodeio.infrastructure.constrained_types import KeyStr
from nodeio.infrastructure.logger import NodeIOLogger


class ListAction(BaseModel, validate_assignment=True):
    """List action configuration. This configuration allows to select an 
    index of a list"""
    index: int
    type: Literal["list"] = "list"


class DictAction(BaseModel, validate_assignment=True):
    """Dictionary action configuration. This configuration allows to select 
    a key of a dictionary"""
    key: KeyStr
    type: Literal["dict"] = "dict"


class InputStream(BaseModel, validate_assignment=True):
    """Input stream configuration. This configuration associates an output
    stream with an input argument."""

    arg: KeyStr
    stream: KeyStr
    actions: Optional[
        list[
            Annotated[
                Union[ListAction, DictAction], Field(discriminator="type")
            ]
        ]
    ] = []


class Node(BaseModel, validate_assignment=True):
    """Node configuration. Defines the node identification, input streams and
    output stream."""

    type: Literal["node"] = "node"
    node: KeyStr
    input_streams: Optional[list[InputStream]] = []
    output_stream: Optional[KeyStr] = None
    options: Optional[dict] = {}

    @model_validator(mode="after")
    def _check_configuration(self) -> Self:
        """Validates if configuration as at least one input stream or an output
         stream.

        :return: Instance of Node
        :rtype: Self

        :raises ValueError: If configuration does not have input streams or an
        output stream defined.
        """
        if len(self.input_streams) == 0 and self.output_stream is not None:
            NodeIOLogger().logger.info("Node %s is a source node.", self.node)
        elif len(self.input_streams) != 0 and self.output_stream is None:
            NodeIOLogger().logger.info("Node %s is a sink node.", self.node)
        elif len(self.input_streams) != 0 and self.output_stream is not None:
            NodeIOLogger().logger.info(
                "Node %s is a processing node.", self.node
            )
        else:  # len(self.input_streams) == 0 and self.output_stream is None:
            error_message = f"Node {self.node} does not participate in the " \
                "graph computation. Please remove node from the configuration."
            NodeIOLogger().logger.error(error_message)
            raise ValueError(error_message)
        return self


class Graph(BaseModel, validate_assignment=True):
    """Graph configuration. Defines the nodes in the graph as well as the main
    input and output streams."""

    input_streams: list[KeyStr]
    output_streams: list[KeyStr]
    nodes: list[Annotated[Union[Node], Field(discriminator="type")]]

    @model_validator(mode="after")
    def _check_configuration(self) -> Self:
        """Validates if configuration as at least one ocurrence of each
        property.

        :return: Instance of Graph
        :rtype: Self

        :raises ValueError: If configuration does not have input streams
        defined.
        :raises ValueError: If configuration does not have output streams
        defined.
        :raises ValueError: If configuration does not have nodes defined.
        """
        if len(self.input_streams) == 0:
            error_message = (
                "No main input streams defined. Please review configuration"
            )
            NodeIOLogger().logger.error(error_message)
            raise ValueError(error_message)

        if len(self.output_streams) == 0:
            error_message = (
                "No main output streams defined. Please review configuration"
            )
            NodeIOLogger().logger.error(error_message)
            raise ValueError(error_message)

        if len(self.nodes) == 0:
            error_message = (
                "No nodes defined for graph. Please review configuration"
            )
            NodeIOLogger().logger.error(error_message)
            raise ValueError(error_message)
        return self
