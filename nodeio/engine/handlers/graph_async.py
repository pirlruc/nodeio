"""The graph async module contains auxiliary functions for asynchronous
 graph processing.

Functions:
    process_graph_async - processes the graph asynchronously.
"""

import asyncio
import sys
from typing import Optional

from pydantic import validate_call

from nodeio.decorators.logging import log
from nodeio.engine.handlers.node_handler import NodeHandler
from nodeio.engine.structures.stream import ContextStream
from nodeio.infrastructure.constants import LOGGING_ENABLED
from nodeio.infrastructure.constrained_types import KeyStr
from nodeio.infrastructure.logger import NodeIOLogger


# TODO: Remove validate_call
@validate_call
@log(enabled=LOGGING_ENABLED)
async def __obtain_process_context_python_311(
    graph: dict[int, list[NodeHandler]],
    context: dict[KeyStr, ContextStream]
) -> dict[KeyStr, ContextStream]:
    """Obtain process context for Python >= 3.11.

    :param graph: Processing graph defined by levels.
    :type graph: dict[int, list[NodeHandler]]
    :param context: Graph processing context
    :type context: dict[KeyStr, ContextStream]

    :return: Processing context from nodes in graph
    :rtype: dict[KeyStr, ContextStream]
    """
    for level, node_handlers in graph.items():
        NodeIOLogger().logger.info(
            "Processing %d main nodes in level %d...",
            len(node_handlers), level
        )
        async with asyncio.TaskGroup() as task_group:
            for node_handler in node_handlers:
                NodeIOLogger().logger.info(
                    "--> Processing node handler %s...",
                    node_handler.name
                )
                task: asyncio.Task = task_group.create_task(
                    node_handler.process_async(context=context)
                )

                def task_done_callback(task: asyncio.Task):
                    """Updates the context after the task has finished
                        successfully.

                    :param task: Asynchronous task to execute a node.
                    :type task: asyncio.Task
                    """
                    context.update(task.result())

                task.add_done_callback(task_done_callback)
    return context


# TODO: Remove validate_call
@validate_call
@log(enabled=LOGGING_ENABLED)
async def __obtain_process_context_python_39(
    graph: dict[int, list[NodeHandler]],
    context: dict[KeyStr, ContextStream]
) -> dict[KeyStr, ContextStream]:
    """Obtain process context for Python < 3.11.

    :param graph: Processing graph defined by levels.
    :type graph: dict[int, list[NodeHandler]]
    :param context: Graph processing context
    :type context: dict[KeyStr, ContextStream]

    :raises RuntimeError: If an error occurs while processing a node

    :return: Processing context from nodes in graph
    :rtype: dict[KeyStr, ContextStream]
    """
    for level, node_handlers in graph.items():
        NodeIOLogger().logger.info(
            "Processing %d main nodes in level %d...",
            len(node_handlers), level
        )
        async_tasks = []
        for node_handler in node_handlers:
            NodeIOLogger().logger.info(
                "--> Processing node handler %s...",
                node_handler.name
            )
            task: asyncio.Task = asyncio.create_task(
                node_handler.process_async(context=context)
            )

            def task_done_callback(task: asyncio.Task):
                """Verifies if the asynchronous task has finished
                    successfully to update context. If not, propagates
                    the error to the main processing.

                :param task: Asynchronous task to execute a node.
                :type task: asyncio.Task

                :raises RuntimeError: Any error that can occur while
                    processing a node.
                """

                error = task.exception()
                if error is not None:
                    error_message = "Error processing node handler " \
                        f"{node_handler.name} in graph: {error}"
                    NodeIOLogger().logger.error(error_message)
                    raise RuntimeError(error_message) from error
                context.update(task.result())

            task.add_done_callback(task_done_callback)
            async_tasks.append(task)
        await asyncio.gather(*async_tasks)
    return context


@validate_call
@log(enabled=LOGGING_ENABLED)
async def process_graph_async(
    graph: dict[int, list[NodeHandler]],
    context: Optional[dict[KeyStr, ContextStream]] = None
) -> dict[KeyStr, ContextStream]:
    """Process graphs assynchronously.

    :param graph: Processing graph defined by levels.
    :type graph: dict[int, list[NodeHandler]]
    :param context: Graph processing context, defaults to None
    :type context: Optional[dict[KeyStr, ContextStream]], optional

    :return: Processing context from nodes in graph
    :rtype: dict[KeyStr, ContextStream]
    """
    if context is None:
        context = {}

    if sys.version_info.major >= 3 and sys.version_info.minor >= 11:
        context = await __obtain_process_context_python_311(
            graph=graph, context=context)
    else:  # < 3.11:
        context = await __obtain_process_context_python_39(
            graph=graph, context=context)
    return context
