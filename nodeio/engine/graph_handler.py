"""The graph handler module contains the objects needed to manage a graph.

Classes:
    GraphHandler - manages a graph network, its nodes and connections. The
     class provides mechanisms to separate the graph into subgraphs, one that
     depends on external nodes (main graph) and other that does not depend on
     external nodes (secondary graph). Each graph can be executed using
     synchronous or assynchronous methods.
"""

from enum import Enum
from typing import Any, Optional

import matplotlib.pyplot
from networkx import (
    DiGraph,
    bfs_layers,
    draw_networkx,
    get_node_attributes,
)
from pydantic import BaseModel, PrivateAttr, validate_call
from typing_extensions import Self

import nodeio.engine.handlers.graph_async
import nodeio.engine.handlers.graph_sync
import nodeio.engine.handlers.graph_utils
from nodeio.decorators.logging import log
from nodeio.decorators.validation import custom_validate_call
from nodeio.engine.configuration import Graph
from nodeio.engine.handlers.node_handler import NodeHandler
from nodeio.engine.handlers.stream_handler import StreamHandler
from nodeio.engine.node_factory import NodeFactory
from nodeio.engine.structures.stream import ContextStream, OutputStream
from nodeio.infrastructure.constants import (
    LOGGING_ENABLED,
    PRIVATE_VALIDATE_CALL_ENABLED,
)
from nodeio.infrastructure.constrained_types import KeyStr
from nodeio.infrastructure.logger import NodeIOLogger


class _NodeType(str, Enum):
    """External node types. These nodes are used to identify the types of
     external nodes that can exist. These nodes are not processing nodes."""
    EXTERNAL_SOURCE_NODE = "external_source_node"
    EXTERNAL_SINK_NODE = "external_sink_node"


class GraphHandler(BaseModel, validate_assignment=True):
    """Manages and handles a processing graph."""
    __graph: DiGraph = PrivateAttr(default=DiGraph())
    __input_streams: list[OutputStream] = PrivateAttr(default=[])
    __output_streams: list[OutputStream] = PrivateAttr(default=[])
    __main_processing_graph: dict[int, list[NodeHandler]] = PrivateAttr(
        default={}
    )
    __secondary_processing_graph: dict[int, list[NodeHandler]] = PrivateAttr(
        default={}
    )
    __secondary_context: dict[KeyStr, ContextStream] = PrivateAttr(
        default={}
    )

    @property
    def number_nodes(self) -> int:
        """Returns the number of nodes registered in handler.

        :return: Number of nodes
        :rtype: int
        """
        return len(self.__graph.nodes)

    @property
    def number_main_processing_graph_nodes(self) -> int:
        """Returns the number of nodes in the main processing graph.

        :return: Number of nodes in main processing graph
        :rtype: int
        """
        return len(
            [
                node
                for _, nodes in self.__main_processing_graph.items()
                for node in nodes
            ]
        )

    @property
    def number_secondary_processing_graph_nodes(self) -> int:
        """Returns the number of nodes in the secondary processing graph.

        :return: Number of nodes in secondary processing graph
        :rtype: int
        """
        return len(
            [
                node
                for _, nodes in self.__secondary_processing_graph.items()
                for node in nodes
            ]
        )

    @property
    def number_input_streams(self) -> int:
        """Returns the number of main input streams registered in handler.

        :return: Number of input streams
        :rtype: int
        """
        return len(self.__input_streams)

    @property
    def number_output_streams(self) -> int:
        """Returns the number of main output streams registered in handler.

        :return: Number of output streams
        :rtype: int
        """
        return len(self.__output_streams)

    @validate_call
    @log(enabled=LOGGING_ENABLED)
    def load(self, factory: NodeFactory, configuration: Graph) -> Self:
        """Loads the configuration for the graph processing handler.

        :param factory: Node factory with the generation functors registered.
        :type factory: NodeFactory
        :param configuration: Graph configuration
        :type configuration: Graph

        :return: Updated graph instance
        :rtype: Self
        """
        self.__create_graph(factory=factory, configuration=configuration)

        node_handlers = get_node_attributes(self.__graph, name="handler")
        self.__main_processing_graph = self.__create_main_processing_graph(
            node_handlers=node_handlers
        )
        self.__secondary_processing_graph = \
            self.__create_secondary_processing_graph(
                node_handlers=node_handlers
            )
        return self

    # TODO: Merge open and open_async methods
    @log(enabled=LOGGING_ENABLED)
    def open(self):
        """Execute nodes independent from the main processing graph
         synchronously. The context will be stored and used by the main
         processing graph.
        """
        NodeIOLogger().logger.info("Processing independent nodes (open "
                                   "method graph)...")
        self.__secondary_context = \
            nodeio.engine.handlers.graph_sync.process_graph(
                graph=self.__secondary_processing_graph
            )

    @log(enabled=LOGGING_ENABLED)
    async def open_async(self):
        """Execute nodes independent from the main processing graph
         assynchronously. The context will be stored and used by the main
         processing graph.
        """
        NodeIOLogger().logger.info("Processing independent nodes (open "
                                   "method graph)...")
        self.__secondary_context = \
            await nodeio.engine.handlers.graph_async.process_graph_async(
                graph=self.__secondary_processing_graph
            )

    # TODO: Merge process and process_async methods
    @validate_call
    @log(enabled=LOGGING_ENABLED)
    def process(
        self, context: Optional[dict[KeyStr, ContextStream]] = None
    ) -> dict[KeyStr, Any]:
        """Executed nodes from the main processing graph.
        The context returned correspond only to the main output streams
        registered in the main processing graph.

        :param context: External processing context, defaults to None
        :type context: Optional[dict[KeyStr, ContextStream]], optional

        :return: Processing context
        :rtype: dict[KeyStr, Any]
        """
        if context is None:
            context = {}

        self.__validate_open_execution()
        nodeio.engine.handlers.graph_utils.validate_streams_in_context(
            streams=self.__input_streams, context=context
        )
        process_context = context
        process_context.update(self.__secondary_context)
        NodeIOLogger().logger.info("Processing nodes from main processing "
                                   "graph...")
        process_context = nodeio.engine.handlers.graph_sync.process_graph(
            context=process_context,
            graph=self.__main_processing_graph
        )
        nodeio.engine.handlers.graph_utils.validate_streams_in_context(
            streams=self.__output_streams, context=process_context
        )
        return self.__filter_output(context=process_context)

    @validate_call
    @log(enabled=LOGGING_ENABLED)
    async def process_async(
        self, context: Optional[dict[KeyStr, ContextStream]] = None
    ) -> dict[KeyStr, Any]:
        """Executed nodes from the main processing graph.
        The context returned correspond only to the main output streams
        registered in the main processing graph.

        :param context: External processing context, defaults to dict()
        :type context: Optional[dict[KeyStr, ContextStream]], optional

        :return: Processing context
        :rtype: dict[KeyStr, Any]
        """
        if context is None:
            context = {}

        self.__validate_open_execution()
        nodeio.engine.handlers.graph_utils.validate_streams_in_context(
            streams=self.__input_streams, context=context
        )
        process_context = context
        process_context.update(self.__secondary_context)
        NodeIOLogger().logger.info("Processing nodes from main processing "
                                   "graph...")
        process_context = \
            await nodeio.engine.handlers.graph_async.process_graph_async(
                context=process_context,
                graph=self.__main_processing_graph
            )
        nodeio.engine.handlers.graph_utils.validate_streams_in_context(
            streams=self.__output_streams, context=process_context
        )
        return self.__filter_output(context=process_context)

    @validate_call
    @log(enabled=LOGGING_ENABLED)
    def write_graph(self, filename: str, options: Optional[dict] = None):
        """Writes the graph into an image file.

        :param filename: Image filename to save the graph
        :type filename: str
        :param options: Options to draw the graph. For more information check
         draw_networkx documentation
        :type options: dict
        """
        if options is None:
            options = {
                "font_size": 14,
                "node_size": 600,
                "node_color": "white",
                "edgecolors": "black",
                "linewidths": 2,
                "width": 2,
            }

        draw_networkx(self.__graph, **options)
        ax = matplotlib.pyplot.gca()
        ax.margins(0.20)
        matplotlib.pyplot.axis("off")
        matplotlib.pyplot.savefig(filename)

    @custom_validate_call(enabled=PRIVATE_VALIDATE_CALL_ENABLED)
    @log(enabled=LOGGING_ENABLED)
    def __create_graph(self, factory: NodeFactory, configuration: Graph):
        """Create nodes and streams for a directed graph.

        :param factory: Node factory with the generation functors registered.
        :type factory: NodeFactory
        :param configuration: Graph configuration
        :type configuration: Graph
        """
        self.__graph.clear()
        stream_handler = StreamHandler(graph=self.__graph)
        node_keys = set()

        # Add nodes to register main input and main output streams
        node_keys = nodeio.engine.handlers.graph_utils.add_node(
            graph=self.__graph,
            node_keys=node_keys,
            node_key=_NodeType.EXTERNAL_SOURCE_NODE.name.lower(),
        )
        node_keys = nodeio.engine.handlers.graph_utils.add_node(
            graph=self.__graph,
            node_keys=node_keys,
            node_key=_NodeType.EXTERNAL_SINK_NODE.name.lower(),
        )

        self.__input_streams = \
            nodeio.engine.handlers.graph_utils.register_input_streams(
                origin=_NodeType.EXTERNAL_SOURCE_NODE.name.lower(),
                stream_handler=stream_handler,
                configuration=configuration.input_streams,
            )
        node_keys = nodeio.engine.handlers.graph_utils.register_node_handlers(
            graph=self.__graph,
            node_keys=node_keys,
            factory=factory,
            stream_handler=stream_handler,
            configuration=configuration.nodes,
        )
        self.__output_streams = \
            nodeio.engine.handlers.graph_utils.register_output_streams(
                ending=_NodeType.EXTERNAL_SINK_NODE.name.lower(),
                stream_handler=stream_handler,
                configuration=configuration.output_streams,
            )
        nodeio.engine.handlers.graph_utils.validate_nodes_and_streams(
            graph=self.__graph,
            stream_handler=stream_handler
        )

    @custom_validate_call(enabled=PRIVATE_VALIDATE_CALL_ENABLED)
    @log(enabled=LOGGING_ENABLED)
    def __create_main_processing_graph(
        self, node_handlers: dict[KeyStr, Optional[NodeHandler]]
    ) -> dict[int, list[NodeHandler]]:
        """Obtain main processing graph with node handlers removing external
        source and sink nodes.

        :param node_handlers: Node handlers.
        :type node_handlers: dict[KeyStr, Optional[NodeHandler]]

        :return: Main processing graph defined by levels.
        :rtype: dict[int, list[NodeHandler]]
        """
        source_nodes = set()
        source_nodes.add(_NodeType.EXTERNAL_SOURCE_NODE.name.lower())
        return nodeio.engine.handlers.graph_utils.create_processing_graph(
            graph=self.__graph,
            source_nodes=source_nodes,
            node_handlers=node_handlers
        )

    @custom_validate_call(enabled=PRIVATE_VALIDATE_CALL_ENABLED)
    @log(enabled=LOGGING_ENABLED)
    def __create_secondary_processing_graph(
        self, node_handlers: dict[KeyStr, Optional[NodeHandler]]
    ) -> dict[int, list[NodeHandler]]:
        """Obtain source processing graph with node handlers.
        This graph is independent from the main processing graph and should be
        executed before the main processing graph.

        :param node_handlers: Node handlers.
        :type node_handlers: dict[KeyStr, Optional[NodeHandler]]

        :return: Source processing graph defined by levels.
        :rtype: dict[int, list[NodeHandler]]
        """
        processing_graph = {}
        secondary_nodes: set = (
            self.__get_nodes_in_secondary_processing_graph()
        )
        if len(secondary_nodes) != 0:
            source_nodes: set = \
                nodeio.engine.handlers.graph_utils.get_source_nodes(
                    graph=self.__graph,
                    nodes=secondary_nodes
                )
            source_nodes.add(_NodeType.EXTERNAL_SOURCE_NODE.name.lower())
            layers: dict = \
                nodeio.engine.handlers.graph_utils.create_processing_graph(
                    graph=self.__graph,
                    source_nodes=source_nodes,
                    node_handlers=node_handlers
                )
            level = 0
            for _, handlers in layers.items():
                increase_level = False
                level_nodes = []
                for handler in handlers:
                    if handler.name not in secondary_nodes:
                        continue
                    secondary_nodes.remove(handler.name)
                    if node_handlers[handler.name] is not None:
                        increase_level = True
                        level_nodes.append(node_handlers[handler.name])

                if increase_level:
                    processing_graph[level] = level_nodes
                    level += 1

                if len(secondary_nodes) == 0:
                    break

        return processing_graph

    @custom_validate_call(enabled=PRIVATE_VALIDATE_CALL_ENABLED)
    @log(enabled=LOGGING_ENABLED)
    def __filter_output(
        self, context: dict[KeyStr, ContextStream]
    ) -> dict[KeyStr, Any]:
        """Filter output from graph processing context.

        :param context: Graph processing context
        :type context: dict[KeyStr, ContextStream]

        :return: Dictionary with the output requested in the processing graph
        :rtype: dict[KeyStr, Any]
        """
        output = {}
        for output_stream in self.__output_streams:
            output[output_stream.key] = context[output_stream.key].get()
        return output

    @log(enabled=LOGGING_ENABLED)
    def __get_nodes_in_secondary_processing_graph(self) -> set[KeyStr]:
        """Obtain nodes in secondary processing graph.

        :return: Nodes in secondary processing graph.
        :rtype: set[KeyStr]
        """
        main_layers = list(
            bfs_layers(
                self.__graph,
                sources=[_NodeType.EXTERNAL_SOURCE_NODE.name.lower()],
            )
        )
        graph_nodes = set(self.__graph.nodes)
        main_nodes = {node for nodes in main_layers for node in nodes}
        return graph_nodes - main_nodes

    @log(enabled=LOGGING_ENABLED)
    def __validate_open_execution(self):
        """Validate if open method is executed before process method.

        :raises RuntimeError: If there are nodes that should be run before the
        main processing graph, the open() method should be executed before
        calling this method.
        """
        if (
            self.number_secondary_processing_graph_nodes != 0
            and len(self.__secondary_context) == 0
        ):
            # TODO: Execute open method and send warning
            error_message = "The nodes independent from the main processing " \
                "graph must be executed before the main processing graph. " \
                "Please perform open() method."
            NodeIOLogger().logger.error(error_message)
            raise RuntimeError(error_message)
