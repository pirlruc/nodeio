import logging

DEFAULT_LOGGING_FORMATTER = logging.Formatter(
    "[%(asctime)s]:[%(levelname)s]:[%(name)s]:%(pathname)s:%(lineno)d: "
    "%(message)s"
)
DEFAULT_LOGGING_LEVEL = logging.DEBUG
