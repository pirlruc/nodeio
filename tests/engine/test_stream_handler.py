import unittest

import networkx
import pydantic_core

from nodeio.engine.stream import OutputStream
from nodeio.engine.stream_handler import StreamHandler


class TestStreamHandler(unittest.TestCase):
    def test_invalid_constructor(self):
        with self.assertRaises(pydantic_core.ValidationError):
            StreamHandler()
        with self.assertRaises(pydantic_core.ValidationError):
            StreamHandler(graph="a")

    def test_constructor(self):
        handler = StreamHandler(graph=networkx.DiGraph())
        self.assertEqual(handler.number_output_streams, 0)
        self.assertFalse(handler.has_unconnected_streams())

    def test_invalid_update(self):
        handler = StreamHandler(graph=networkx.DiGraph())
        with self.assertRaises(pydantic_core.ValidationError):
            handler.graph = networkx.DiGraph()

    def test_add_output_stream(self):
        handler = StreamHandler(graph=networkx.DiGraph())
        handler.add_output_stream(stream=OutputStream(key="a"), origin="a")
        self.assertEqual(handler.number_output_streams, 1)
        self.assertTrue(handler.has_unconnected_streams())

    def test_duplicated_output_stream(self):
        handler = StreamHandler(graph=networkx.DiGraph())
        handler.add_output_stream(stream=OutputStream(key="a"), origin="a")
        with self.assertRaises(KeyError):
            handler.add_output_stream(stream=OutputStream(key="a"), origin="b")

    def test_get_non_existent_output_stream(self):
        handler = StreamHandler(graph=networkx.DiGraph())
        with self.assertRaises(KeyError):
            handler.get_output_stream(key="a")

    def test_get_existent_output_stream(self):
        handler = StreamHandler(graph=networkx.DiGraph())
        handler.add_output_stream(stream=OutputStream(key="a"), origin="a")
        self.assertEqual(handler.get_output_stream(
            key="a"), OutputStream(key="a"))

    def test_connection_non_existent_output_stream(self):
        handler = StreamHandler(graph=networkx.DiGraph())
        with self.assertRaises(KeyError):
            handler.register_connection(key="a", ending="b")

    def test_connection_origin_ending_same(self):
        handler = StreamHandler(graph=networkx.DiGraph())
        handler.add_output_stream(stream=OutputStream(key="a"), origin="a")
        with self.assertRaises(ValueError):
            handler.register_connection(key="a", ending="a")

    def test_register_connection(self):
        handler = StreamHandler(graph=networkx.DiGraph())
        handler.add_output_stream(stream=OutputStream(key="a"), origin="a")
        self.assertTrue(handler.has_unconnected_streams())
        handler.register_connection(key="a", ending="b")
        self.assertFalse(handler.has_unconnected_streams())

    def test_unconnected_streams(self):
        handler = StreamHandler(graph=networkx.DiGraph())
        handler.add_output_stream(stream=OutputStream(key="a"), origin="a")
        handler.add_output_stream(stream=OutputStream(key="b"), origin="a")
        self.assertTrue(handler.has_unconnected_streams())
        self.assertEqual(len(handler.get_unconnected_stream_keys()), 2)
        handler.register_connection(key="a", ending="b")
        self.assertTrue(handler.has_unconnected_streams())
        self.assertEqual(len(handler.get_unconnected_stream_keys()), 1)
        handler.register_connection(key="b", ending="b")
        self.assertFalse(handler.has_unconnected_streams())
        self.assertEqual(len(handler.get_unconnected_stream_keys()), 0)
