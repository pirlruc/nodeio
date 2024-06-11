"""The constants module define constants to be used in the graph
 processing engine.

Constants:
    DEFAULT_LOGGING_FORMATTER - default format for the node engine logger.
    DEFAULT_LOGGING_LEVEL - default level for the node engine logger.
"""

import logging

DEFAULT_LOGGING_FORMATTER = logging.Formatter(
    "[%(asctime)s]:[%(levelname)s]:[%(name)s]:%(pathname)s:%(lineno)d: "
    "%(message)s"
)
DEFAULT_LOGGING_LEVEL = logging.DEBUG
