import logging
from pydantic import BaseModel, PrivateAttr
from typing_extensions import Self
from nodeio.infrastructure.constants import DEFAULT_LOGGING_FORMATTER, DEFAULT_LOGGING_LEVEL

class NodeIOLogger(BaseModel, validate_assignment = True):
    """NodeIO logger object"""
    __logger: logging.Logger = PrivateAttr(default=None)

    def __init__(self, **data):
        """
        Creates and associates a logger singleton object with the class.
        """
        super().__init__(**data)
        self.__logger = logging.getLogger(self.__class__.__name__)

    @property
    def logger(self) -> logging.Logger:
        """
        Returns a logger singleton object.

        :return: Logger singleton object associated with the class.
        :rtype: logging.Logger
        """
        return self.__logger

    def add_default_handler(self) -> Self:
        """
        Associates a default handler with the logger singleton object.
        """
        file_handler = logging.FileHandler(self.__class__.__name__ + ".log", mode = "w")
        file_handler.setFormatter(DEFAULT_LOGGING_FORMATTER)
        self.__logger.addHandler(file_handler)
        self.__logger.setLevel(DEFAULT_LOGGING_LEVEL)
        return self
