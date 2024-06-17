"""The graph sync module contains auxiliary functions for synchronous
 graph processing.

Functions:
    process_graph - processes the graph synchronously.
"""

from typing import Optional

from pydantic import validate_call

from nodeio.decorators.logging import log
from nodeio.engine.handlers.node_handler import NodeHandler
from nodeio.engine.structures.stream import ContextStream
from nodeio.infrastructure.constants import LOGGING_ENABLED
from nodeio.infrastructure.constrained_types import KeyStr
from nodeio.infrastructure.logger import NodeIOLogger


@validate_call
@log(enabled=LOGGING_ENABLED)
def process_graph(
    graph: dict[int, list[NodeHandler]],
    context: Optional[dict[KeyStr, ContextStream]] = None
) -> dict[KeyStr, ContextStream]:
    """Process graphs synchronously.

    :param graph: Processing graph defined by levels.
    :type graph: dict[int, list[NodeHandler]]
    :param context: Graph processing context, defaults to None
    :type context: Optional[dict[KeyStr, ContextStream]], optional

    :return: Processing context from nodes in graph
    :rtype: dict[KeyStr, ContextStream]
    """
    if context is None:
        context = {}

    for level, node_handlers in graph.items():
        NodeIOLogger().logger.info(
            "Processing %d nodes in level %d...",
            len(node_handlers), level
        )
        for node_handler in node_handlers:
            NodeIOLogger().logger.info(
                "--> Processing node handler %s...", node_handler.name
            )
            context = node_handler.process(context=context)
    return context
