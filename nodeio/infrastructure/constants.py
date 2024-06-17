"""The constants module define constants to be used in the graph
 processing engine.

Constants:
    DEFAULT_LOGGING_FORMATTER - default format for the node engine logger.
    DEFAULT_LOGGING_LEVEL - default level for the node engine logger.
    LOGGING_ENABLED - flag that indicates if the library logging is enabled.
"""

import logging
import os

DEFAULT_LOGGING_FORMATTER = logging.Formatter(
    "[%(asctime)s]:[%(levelname)s]:[%(name)s]:%(pathname)s:%(lineno)d: "
    "%(message)s"
)
DEFAULT_LOGGING_LEVEL = logging.DEBUG

LOGGING_ENABLED = os.environ.get("NODEIO_LIB_LOGGING", False)
if ~isinstance(LOGGING_ENABLED, bool):
    LOGGING_ENABLED = False
