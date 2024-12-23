import logging
import os
import unittest

from nodeio.infrastructure.constants import (
    DEFAULT_LOGGING_FORMATTER,
    DEFAULT_LOGGING_LEVEL,
)
from nodeio.infrastructure.logger import NodeIOLogger


class TestNodeIOLogger(unittest.TestCase):
    def test_singleton_creation(self):
        self.assertEqual(id(NodeIOLogger()), id(NodeIOLogger()))
        self.assertEqual(DEFAULT_LOGGING_LEVEL, logging.DEBUG)

    def test_logging(self):
        log1 = NodeIOLogger()
        file_handler = logging.FileHandler('test.log', 'w')
        file_handler.setFormatter(DEFAULT_LOGGING_FORMATTER)

        log1.logger.addHandler(file_handler)
        log1.logger.setLevel(logging.INFO)
        log1.logger.debug('test_debug_level')
        log1.logger.info('test_info_level')
        log1.logger.error('test_error_level')
        log1.logger.removeHandler(file_handler)
        file_handler.close()
        fp = open('test.log')
        lines = fp.readlines()
        fp.close()
        self.assertEqual(len(lines), 2)
        os.remove('test.log')

    def test_logging_default(self):
        log1 = NodeIOLogger()
        log1.add_default_handler()
        log1.logger.debug('test_debug_level')
        log1.logger.info('test_info_level')
        log1.logger.error('test_error_level')
        log1.logger.handlers.clear()
        fp = open('NodeIOLogger.log')
        lines = fp.readlines()
        fp.close()
        self.assertEqual(len(lines), 3)
        os.remove('NodeIOLogger.log')
