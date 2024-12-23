"""The graph utils module contains auxiliary functions for interacting
 with graphs.

Functions:
    add_node - adds a node to a graph.
    register_input_streams - registers main input streams in a graph.
    register_node_handlers - registers node handlers in a graph.
    register_output_streams - registers main output streams in a graph.
    validate_nodes_and_streams - validates graph nodes and streams to ensure
     connections between nodes.
    create_processing_graph - creates a leveled processing graph.
    get_source_nodes - checks source nodes within a list of nodes.
    validate_streams_in_context - checks if streams are present and have a
     value in context.
"""

from typing import Optional

from networkx import DiGraph, isolates
from pydantic import ConfigDict, validate_call

from nodeio.decorators.logging import log
from nodeio.engine.base_node import BaseNode
from nodeio.engine.configuration import Node
from nodeio.engine.handlers.node_handler import NodeHandler
from nodeio.engine.handlers.stream_handler import StreamHandler
from nodeio.engine.node_factory import NodeFactory
from nodeio.engine.structures.stream import ContextStream, OutputStream
from nodeio.infrastructure.constants import LOGGING_ENABLED
from nodeio.infrastructure.constrained_types import KeyStr
from nodeio.infrastructure.exceptions import ConfigurationError
from nodeio.infrastructure.logger import NodeIOLogger


@validate_call(config=ConfigDict(arbitrary_types_allowed=True))
@log(enabled=LOGGING_ENABLED)
def add_node(
    graph: DiGraph,
    node_keys: set[KeyStr],
    node_key: KeyStr,
    handler: Optional[NodeHandler] = None,
) -> set[KeyStr]:
    """Add node to graph.

    :param graph: Graph to add node.
    :type graph: DiGraph
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
        error_message = (
            f'Node with identifier {node_key} already ' 'exists. Please review configuration'
        )
        NodeIOLogger().logger.error(error_message)
        raise ConfigurationError(error_message)
    graph.add_node(node_for_adding=node_key, handler=handler)
    node_keys.add(node_key)
    return node_keys


@validate_call
@log(enabled=LOGGING_ENABLED)
def register_input_streams(
    origin: KeyStr, stream_handler: StreamHandler, configuration: list[KeyStr]
) -> list[OutputStream]:
    """Register input streams in stream handler.

    :param origin: Key of the node that originates the output stream.
    :type origin: KeyStr
    :param stream_handler: Handler for input and output streams.
    :type stream_handler: StreamHandler
    :param configuration: Main input streams configuration.
    :type configuration: list[KeyStr]

    :return: List of main input streams (treated as output streams in
     the framework)
    :rtype: list[OutputStream]
    """
    # Main input streams are considered output streams in the framework
    # Therefore, they are registered in the stream handler as output
    # streams
    input_streams = []
    for input_stream_key in configuration:
        input_stream = OutputStream(key=input_stream_key)
        input_streams.append(input_stream)
        stream_handler.add_output_stream(stream=input_stream, origin=origin)
    return input_streams


@validate_call(config=ConfigDict(arbitrary_types_allowed=True))
@log(enabled=LOGGING_ENABLED)
def register_node_handlers(
    graph: DiGraph,
    node_keys: set,
    factory: NodeFactory,
    stream_handler: StreamHandler,
    configuration: list[Node],
) -> set:
    """Obtain node handlers from configuration and register them in the
    graph.

    :param graph: Graph to register nodes.
    :type graph: DiGraph
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
        node: BaseNode = factory.create(key=node_config.type, **input_context)
        handler: NodeHandler = NodeHandler.from_configuration(
            functor=node.process,
            stream_handler=stream_handler,
            configuration=node_config,
        )
        node_keys = add_node(
            graph=graph,
            node_keys=node_keys,
            node_key=handler.name,
            handler=handler,
        )
    return node_keys


@validate_call
@log(enabled=LOGGING_ENABLED)
def register_output_streams(
    ending: KeyStr, stream_handler: StreamHandler, configuration: list[KeyStr]
) -> list[OutputStream]:
    """Register output streams in stream handler.

    :param ending: Key of the node that is registering the connection
    :type ending: KeyStr
    :param stream_handler: Handler for input and output streams.
    :type stream_handler: StreamHandler
    :param configuration: Main output streams configuration.
    :type configuration: list[KeyStr]

    :raises ConfigurationError: If the main output stream does not have a
        connection with a node.

    :return: List of main output streams (treated as input streams in
     the framework)
    :rtype: list[OutputStream]
    """
    # Main output streams are considered input streams in the framework
    # Therefore, they are not registered in the stream handler.
    # They should register connections with existent output streams
    output_streams = []
    for output_stream_key in configuration:
        try:
            output_stream = stream_handler.get_output_stream(key=output_stream_key)
        except KeyError as error:
            error_message = (
                f'Main output stream {output_stream_key} '
                'does not have a registered connection with a node. '
                'Please review configuration'
            )
            NodeIOLogger().logger.error(error_message)
            raise ConfigurationError(error_message) from error

        output_streams.append(output_stream)
        stream_handler.register_connection(key=output_stream_key, ending=ending)
    return output_streams


@validate_call(config=ConfigDict(arbitrary_types_allowed=True))
@log(enabled=LOGGING_ENABLED)
def validate_nodes_and_streams(graph: DiGraph, stream_handler: StreamHandler):
    """Validates if the graph has no isolated nodes and all stream have
    connections.

    :param graph: Graph to validate.
    :type graph: DiGraph
    :param stream_handler: Handler for input and output streams.
    :type stream_handler: StreamHandler

    :raises ConfigurationError: If the graph has isolated nodes or
    has streams without connections.
    """
    isolated_nodes = list(isolates(graph))
    error_message = ''
    if len(isolated_nodes) != 0:
        isolated_nodes_str = ''
        for node_key in isolated_nodes:
            if isolated_nodes_str == '':
                isolated_nodes_str += f'{node_key}'
            else:
                isolated_nodes_str += f', {node_key}'
        error_message = (
            f'There are {len(isolated_nodes)} isolated ' f'nodes: {isolated_nodes_str}. '
        )

    if stream_handler.has_unconnected_streams():
        unconnected_streams = stream_handler.get_unconnected_stream_keys()
        unconnected_streams_str = ''
        for stream_key in unconnected_streams:
            if unconnected_streams_str == '':
                unconnected_streams_str += f'{stream_key}'
            else:
                unconnected_streams_str += f', {stream_key}'
        error_message += (
            f'There are {len(unconnected_streams)} '
            f'unconnected streams: {unconnected_streams_str}. '
        )

    if len(isolated_nodes) != 0 or stream_handler.has_unconnected_streams():
        error_message += 'Please review configuration'
        NodeIOLogger().logger.error(error_message)
        raise ConfigurationError(error_message)


@validate_call(config=ConfigDict(arbitrary_types_allowed=True))
@log(enabled=LOGGING_ENABLED)
def create_processing_graph(
    graph: DiGraph,
    source_nodes: set[KeyStr],
    node_handlers: dict[KeyStr, Optional[NodeHandler]],
) -> dict[int, list[NodeHandler]]:
    """Obtain processing graph with node handlers removing external source
    and sink nodes.

    :param graph: Source graph to create processing graph.
    :type graph: DiGraph
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
            prev_nodes = set(graph.predecessors(node_key))
            next_nodes = set(graph.successors(node_key))
            if any(prev_node in iterator_nodes for prev_node in prev_nodes):
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


@validate_call(config=ConfigDict(arbitrary_types_allowed=True))
@log(enabled=LOGGING_ENABLED)
def get_source_nodes(graph: DiGraph, nodes: set[KeyStr]) -> set[KeyStr]:
    """Obtain source nodes. This corresponds to nodes that do not have
    input streams.

    :param graph: Graph to obtain source nodes.
    :type graph: DiGraph
    :param nodes: List of nodes
    :type nodes: set[KeyStr]

    :return: Source nodes.
    :rtype: set[KeyStr]
    """
    return {node_key for node_key in nodes if len(graph.in_edges(node_key)) == 0}


@validate_call
@log(enabled=LOGGING_ENABLED)
def validate_streams_in_context(
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
        if stream.key not in context or not context[stream.key].has_value():
            error_message = 'Context does not have the stream ' f'{stream.key} value loaded.'
            NodeIOLogger().logger.error(error_message)
            raise KeyError(error_message)
