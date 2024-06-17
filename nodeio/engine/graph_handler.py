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
    isolates,
)
from pydantic import BaseModel, PrivateAttr, validate_call
from typing_extensions import Self

import nodeio.engine.handlers.graph_async
import nodeio.engine.handlers.graph_sync
from nodeio.decorators.logging import log
from nodeio.engine.base_node import BaseNode
from nodeio.engine.configuration import Graph, Node
from nodeio.engine.handlers.node_handler import NodeHandler
from nodeio.engine.handlers.stream_handler import StreamHandler
from nodeio.engine.node_factory import NodeFactory
from nodeio.engine.structures.stream import ContextStream, OutputStream
from nodeio.infrastructure.constants import LOGGING_ENABLED
from nodeio.infrastructure.constrained_types import KeyStr
from nodeio.infrastructure.exceptions import ConfigurationError
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
        self.__graph.clear()
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
        self.__validate_streams_in_context(
            streams=self.__input_streams, context=context
        )
        process_context = context.copy()
        process_context.update(self.__secondary_context)
        NodeIOLogger().logger.info("Processing nodes from main processing "
                                   "graph...")
        process_context = nodeio.engine.handlers.graph_sync.process_graph(
            context=process_context,
            graph=self.__main_processing_graph
        )
        self.__validate_streams_in_context(
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
        self.__validate_streams_in_context(
            streams=self.__input_streams, context=context
        )
        process_context = context.copy()
        process_context.update(self.__secondary_context)
        NodeIOLogger().logger.info("Processing nodes from main processing "
                                   "graph...")
        process_context = \
            await nodeio.engine.handlers.graph_async.process_graph_async(
                context=process_context,
                graph=self.__main_processing_graph
            )
        self.__validate_streams_in_context(
            streams=self.__output_streams, context=process_context
        )
        return self.__filter_output(context=process_context)

    @validate_call
    @log(enabled=LOGGING_ENABLED)
    def write_graph(self, filename: str):
        """Writes the graph into an image file.

        :param filename: Image filename to save the graph
        :type filename: str
        """
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

    # TODO: Remove validate_call
    @validate_call
    @log(enabled=LOGGING_ENABLED)
    def __create_graph(self, factory: NodeFactory, configuration: Graph):
        """Create nodes and streams for a directed graph.

        :param factory: Node factory with the generation functors registered.
        :type factory: NodeFactory
        :param configuration: Graph configuration
        :type configuration: Graph
        """
        stream_handler = StreamHandler(graph=self.__graph)
        node_keys = set()

        # Add nodes to register main input and main output streams
        node_keys = self.__add_node(
            node_keys=node_keys,
            node_key=_NodeType.EXTERNAL_SOURCE_NODE.name.lower(),
        )
        node_keys = self.__add_node(
            node_keys=node_keys,
            node_key=_NodeType.EXTERNAL_SINK_NODE.name.lower(),
        )

        self.__register_input_streams(
            stream_handler=stream_handler,
            configuration=configuration.input_streams,
        )
        node_keys = self.__register_nodes(
            node_keys=node_keys,
            factory=factory,
            stream_handler=stream_handler,
            configuration=configuration.nodes,
        )
        self.__register_output_streams(
            stream_handler=stream_handler,
            configuration=configuration.output_streams,
        )
        self.__validate_nodes_and_streams(stream_handler=stream_handler)

    # TODO: Remove validate_call
    @validate_call
    @log(enabled=LOGGING_ENABLED)
    def __add_node(
        self,
        node_keys: set[KeyStr],
        node_key: KeyStr,
        handler: Optional[NodeHandler] = None,
    ) -> set[KeyStr]:
        """Add node to graph.

        :param node_keys: Set of node keys already in graph.
        :type node_keys: set[KeyStr]
        :param node_key: Node key to be inserted in the graph.
        :type node_key: KeyStr
        :param handler: Node handler for processing, defaults to None
        :type handler: Optional[NodeHandler], optional

        :raises ConfigurationError: Node key being inserted already exists
         in graph.

        :return: Updated set of node keys in graph
        :rtype: set[KeyStr]
        """
        if node_key in node_keys:
            error_message = f"Node with identifier {node_key} already " \
                "exists. Please review configuration"
            NodeIOLogger().logger.error(error_message)
            raise ConfigurationError(error_message)
        self.__graph.add_node(node_for_adding=node_key, handler=handler)
        node_keys.add(node_key)
        return node_keys

    # TODO: Remove validate_call
    @validate_call
    @log(enabled=LOGGING_ENABLED)
    def __register_input_streams(
        self, stream_handler: StreamHandler, configuration: list[KeyStr]
    ):
        """Register input streams in stream handler.

        :param stream_handler: Handler for input and output streams.
        :type stream_handler: StreamHandler
        :param configuration: Main input streams configuration.
        :type configuration: list[KeyStr]
        """
        # Main input streams are considered output streams in the framework
        # Therefore, they are registered in the stream handler as output
        # streams
        for input_stream_key in configuration:
            input_stream = OutputStream(key=input_stream_key)
            self.__input_streams.append(input_stream)
            stream_handler.add_output_stream(
                stream=input_stream,
                origin=_NodeType.EXTERNAL_SOURCE_NODE.name.lower(),
            )

    # TODO: Remove validate_call
    @validate_call
    @log(enabled=LOGGING_ENABLED)
    def __register_nodes(
        self,
        node_keys: set,
        factory: NodeFactory,
        stream_handler: StreamHandler,
        configuration: list[Node],
    ) -> set:
        """Obtain node handlers from configuration and register them in the
        graph.

        :param node_keys: Set of node keys already in graph.
        :type node_keys: set
        :param factory: Node factory with the generation functors registered.
        :type factory: NodeFactory
        :param stream_handler: Handler for input and output streams.
        :type stream_handler: StreamHandler
        :param configuration: List of configurations for nodes.
        :type configuration: list[Node]

        :return: Updated set of node keys in graph
        :rtype: set
        """
        # The nodes will insert input and output streams through the stream
        # handler
        for node_config in configuration:
            input_context = {}
            for key, value in node_config.options.items():
                input_context[key] = value
            node: BaseNode = factory.create(
                key=node_config.type, **input_context
            )
            handler: NodeHandler = NodeHandler.from_configuration(
                functor=node.process,
                stream_handler=stream_handler,
                configuration=node_config
            )
            node_keys = self.__add_node(
                node_keys=node_keys, node_key=handler.name, handler=handler
            )
        return node_keys

    # TODO: Remove validate_call
    @validate_call
    @log(enabled=LOGGING_ENABLED)
    def __register_output_streams(
        self, stream_handler: StreamHandler, configuration: list[KeyStr]
    ):
        """Register output streams in stream handler.

        :param stream_handler: Handler for input and output streams.
        :type stream_handler: StreamHandler
        :param configuration: Main output streams configuration.
        :type configuration: list[KeyStr]

        :raises ConfigurationError: If the main output stream does not have a
         connection with a node.
        """
        # Main output streams are considered input streams in the framework
        # Therefore, they are not registered in the stream handler.
        # They should register connections with existent output streams
        for output_stream_key in configuration:
            try:
                output_stream = stream_handler.get_output_stream(
                    key=output_stream_key
                )
            except KeyError as error:
                error_message = f"Main output stream {output_stream_key} " \
                    "does not have a registered connection with a node. " \
                    "Please review configuration"
                NodeIOLogger().logger.error(error_message)
                raise ConfigurationError(error_message) from error

            self.__output_streams.append(output_stream)
            stream_handler.register_connection(
                key=output_stream_key,
                ending=_NodeType.EXTERNAL_SINK_NODE.name.lower(),
            )

    # TODO: Remove validate_call
    @validate_call
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
        return self.__create_processing_graph(
            source_nodes=source_nodes, node_handlers=node_handlers
        )

    # TODO: Remove validate_call
    @validate_call
    @log(enabled=LOGGING_ENABLED)
    def __create_processing_graph(
        self,
        source_nodes: set[KeyStr],
        node_handlers: dict[KeyStr, Optional[NodeHandler]],
    ) -> dict[int, list[NodeHandler]]:
        """Obtain processing graph with node handlers removing external source
        and sink nodes.

        :param node_handlers: Node handlers.
        :type node_handlers: dict[KeyStr, Optional[NodeHandler]]

        :return: Processing graph defined by levels.
        :rtype: dict[int, list[NodeHandler]]
        """
        processing_graph = {}
        level = 0
        pending_nodes = source_nodes
        while len(pending_nodes) != 0:
            increase_level = False
            level_nodes = []
            iterator_nodes = pending_nodes.copy()
            for node_key in iterator_nodes:
                prev_nodes = set(self.__graph.predecessors(node_key))
                next_nodes = set(self.__graph.successors(node_key))
                if any(
                    prev_node in iterator_nodes for prev_node in prev_nodes
                ):
                    continue

                pending_nodes.remove(node_key)
                pending_nodes.update(next_nodes)
                if node_handlers[node_key] is not None:
                    increase_level = True
                    level_nodes.append(node_handlers[node_key])

            if increase_level:
                processing_graph[level] = level_nodes
                level += 1

        return processing_graph

    # TODO: Remove validate_call
    @validate_call
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
            self.__obtain_nodes_in_secondary_processing_graph()
        )
        if len(secondary_nodes) != 0:
            source_nodes = self.__obtain_source_nodes(nodes=secondary_nodes)
            source_nodes.add(_NodeType.EXTERNAL_SOURCE_NODE.name.lower())
            layers: dict = self.__create_processing_graph(
                source_nodes=source_nodes, node_handlers=node_handlers
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

    # TODO: Remove validate_call
    @validate_call
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

    # TODO: Remove validate_call
    @log(enabled=LOGGING_ENABLED)
    def __obtain_nodes_in_secondary_processing_graph(self) -> set[KeyStr]:
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

    # TODO: Remove validate_call
    @validate_call
    @log(enabled=LOGGING_ENABLED)
    def __obtain_source_nodes(self, nodes: set[KeyStr]) -> list[KeyStr]:
        """Obtain source nodes. This corresponds to nodes that do not have
        input streams.

        :param nodes: List of nodes
        :type nodes: set[KeyStr]

        :return: Source nodes.
        :rtype: list[KeyStr]
        """
        return {
            node_key
            for node_key in nodes
            if len(self.__graph.in_edges(node_key)) == 0
        }

    # TODO: Remove validate_call
    @validate_call
    @log(enabled=LOGGING_ENABLED)
    def __validate_nodes_and_streams(self, stream_handler: StreamHandler):
        """Validates if the graph has no isolated nodes and all stream have
        connections.

        :param stream_handler: Handler for input and output streams.
        :type stream_handler: StreamHandler

        :raises ConfigurationError: If the graph has isolated nodes or
         has streams without connections.
        """
        isolated_nodes = list(isolates(self.__graph))
        error_message = ""
        if len(isolated_nodes) != 0:
            isolated_nodes_str = ""
            for node_key in isolated_nodes:
                if isolated_nodes_str == "":
                    isolated_nodes_str += f"{node_key}"
                else:
                    isolated_nodes_str += f", {node_key}"
            error_message = f"There are {len(isolated_nodes)} isolated " \
                f"nodes: {isolated_nodes_str}. "

        if stream_handler.has_unconnected_streams():
            unconnected_streams = stream_handler.get_unconnected_stream_keys()
            unconnected_streams_str = ""
            for stream_key in unconnected_streams:
                if unconnected_streams_str == "":
                    unconnected_streams_str += f"{stream_key}"
                else:
                    unconnected_streams_str += f", {stream_key}"
            error_message += f"There are {len(unconnected_streams)} " \
                f"unconnected streams: {unconnected_streams_str}. "

        if (
            len(isolated_nodes) != 0
            or stream_handler.has_unconnected_streams()
        ):
            error_message += "Please review configuration"
            NodeIOLogger().logger.error(error_message)
            raise ConfigurationError(error_message)

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
            error_message = "The nodes independent from the main processing " \
                "graph must be executed before the main processing graph. " \
                "Please perform open() method."
            NodeIOLogger().logger.error(error_message)
            raise RuntimeError(error_message)

    # TODO: Remove validate_call
    @staticmethod
    @validate_call
    @log(enabled=LOGGING_ENABLED)
    def __validate_streams_in_context(
        streams: list[OutputStream],
        context: dict[KeyStr, ContextStream],
    ):
        """Validate if streams are present and have a value in context.

        :param streams: List of streams that should exist and have a value in
        context
        :type streams: list[OutputStream]
        :param context: Graph processing context
        :type context: dict[KeyStr, ContextStream]

        :raises KeyError: If stream is not present or it does not have a value
        in context
        """
        for stream in streams:
            if (
                stream.key not in context
                or not context[stream.key].has_value()
            ):
                error_message = "Context does not have the stream " \
                    f"{stream.key} value loaded."
                NodeIOLogger().logger.error(error_message)
                raise KeyError(error_message)
