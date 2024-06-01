import unittest
import pydantic_core
import nodeio.engine.configuration
from nodeio.engine.stream import OutputStream, InputStream, ContextStream
from nodeio.engine.arguments import InputArg
from nodeio.engine.action import ListAction, DictAction

class TestOutputStream(unittest.TestCase):
    def test_invalid_constructor(self):
        with self.assertRaises(pydantic_core.ValidationError):
            OutputStream(key="")
            OutputStream(key="  ")
            OutputStream(key="a",type=1)

    def test_constructor(self):
        stream_1 = OutputStream(key="a")
        stream_2 = OutputStream(key="b",type=int)
        self.assertEqual(stream_1.key,"a")
        self.assertIsNone(stream_1.type)
        self.assertEqual(stream_2.key,"b")
        self.assertEqual(stream_2.type,int)

    def test_invalid_update(self):
        stream = OutputStream(key="b",type=int)
        with self.assertRaises(pydantic_core.ValidationError):
            stream.key = ""
            stream.key = "  "
            stream.type = 1

    def test_update(self):
        stream = OutputStream(key="b",type=int)
        stream.key = "c"
        stream.type = bool
        self.assertEqual(stream.key,"c")
        self.assertEqual(stream.type,bool)

class TestInputStream(unittest.TestCase):
    def test_invalid_constructor(self):
        with self.assertRaises(pydantic_core.ValidationError):
            InputStream(arg=1)
            InputStream(arg=InputArg(key="a"))
            InputStream(arg=InputArg(key="a"),stream=1)
            InputStream(arg=InputArg(key="a"),stream=OutputStream(key="a"),actions=[1])

    def test_constructor(self):
        stream_1 = InputStream(arg=InputArg(key="a"),stream=OutputStream(key="a"))
        stream_2 = InputStream(arg=InputArg(key="b"),stream=OutputStream(key="b"),actions=[ListAction(value=0)])
        self.assertEqual(stream_1.arg,InputArg(key="a"))
        self.assertEqual(stream_1.stream,OutputStream(key="a"))
        self.assertEqual(stream_1.actions,list())
        self.assertEqual(stream_2.arg,InputArg(key="b"))
        self.assertEqual(stream_2.stream,OutputStream(key="b"))
        self.assertEqual(stream_2.actions,[ListAction(value=0)])

    def test_invalid_update(self):
        stream = InputStream(arg=InputArg(key="a"),stream=OutputStream(key="a"))
        with self.assertRaises(pydantic_core.ValidationError):
            stream.arg = 1
            stream.stream = 1
            stream.actions = 1

    def test_update(self):
        stream = InputStream(arg=InputArg(key="a"),stream=OutputStream(key="a"))
        stream.arg = InputArg(key="b")
        stream.stream = OutputStream(key="b")
        stream.actions = [DictAction(value="c")]
        self.assertEqual(stream.arg,InputArg(key="b"))
        self.assertEqual(stream.stream,OutputStream(key="b"))
        self.assertEqual(stream.actions,[DictAction(value="c")])

    def test_complete_input_arg(self):
        stream_1 = InputStream(arg=InputArg(key="a"),stream=OutputStream(key="a",type=int))
        stream_2 = InputStream(arg=InputArg(key="a"),stream=OutputStream(key="a",type=int),actions=[ListAction(value=1)])
        self.assertEqual(stream_1.arg.type,int)
        self.assertIsNone(stream_2.arg.type)
        
    def test_complete_output_stream(self):
        stream_1 = InputStream(arg=InputArg(key="a",type=int),stream=OutputStream(key="a"))
        stream_2 = InputStream(arg=InputArg(key="a",type=int),stream=OutputStream(key="a"),actions=[ListAction(value=1)])
        self.assertEqual(stream_1.stream.type,int)
        self.assertIsNone(stream_2.stream.type)

    def test_complete_output_stream_outside(self):
        output = OutputStream(key="a")
        stream = InputStream(arg=InputArg(key="a",type=int),stream=output)
        self.assertEqual(stream.stream.type,int)
        self.assertEqual(output.type,int)

    def test_invalid_data_types(self):
        InputStream(arg=InputArg(key="a",type=int),stream=OutputStream(key="a",type=bool),actions=[ListAction(value=1)])
        with self.assertRaises(TypeError):
            InputStream(arg=InputArg(key="a",type=int),stream=OutputStream(key="a",type=bool))

    def test_valid_data_types(self):
        stream_1 = InputStream(arg=InputArg(key="a",type=int),stream=OutputStream(key="a",type=int))
        stream_2 = InputStream(arg=InputArg(key="a",type=int),stream=OutputStream(key="a"),actions=[ListAction(value=1)])
        self.assertEqual(stream_1.arg.type,int)
        self.assertEqual(stream_1.stream.type,int)
        self.assertEqual(stream_1.actions,list())
        self.assertEqual(stream_2.arg.type,int)
        self.assertIsNone(stream_2.stream.type,int)
        self.assertEqual(stream_2.actions,[ListAction(value=1)])

    def test_from_configuration(self):
        config_1 = nodeio.engine.configuration.InputStream(arg="a", stream="b")
        stream_1: InputStream = InputStream.from_configuration(configuration=config_1)
        config_2 = nodeio.engine.configuration.InputStream(
            arg="c", 
            stream="d", 
            actions=[nodeio.engine.configuration.DictAction(key="e"),nodeio.engine.configuration.ListAction(index=1)])
        stream_2: InputStream = InputStream.from_configuration(configuration=config_2)
        self.assertEqual(stream_1.arg.key,"a")
        self.assertEqual(stream_1.stream.key,"b")
        self.assertEqual(stream_1.actions,list())
        self.assertEqual(stream_2.arg.key,"c")
        self.assertEqual(stream_2.stream.key,"d")
        self.assertEqual(len(stream_2.actions),2)

class TestContextStream(unittest.TestCase):
    def test_invalid_constructor(self):
        with self.assertRaises(pydantic_core.ValidationError):
            ContextStream(key="")
            ContextStream(key="  ")
            ContextStream(key="a",type=1)

    def test_constructor(self):
        stream_1 = ContextStream(key="a")
        stream_2 = ContextStream(key="b",type=int)
        self.assertEqual(stream_1.key,"a")
        self.assertIsNone(stream_1.type)
        self.assertEqual(stream_2.key,"b")
        self.assertEqual(stream_2.type,int)

    def test_invalid_update(self):
        stream = ContextStream(key="b",type=int)
        with self.assertRaises(pydantic_core.ValidationError):
            stream.key = ""
            stream.key = "  "
            stream.type = 1

    def test_update(self):
        stream = ContextStream(key="b",type=int)
        stream.key = "c"
        stream.type = bool
        self.assertEqual(stream.key,"c")
        self.assertEqual(stream.type,bool)

    def test_invalid_register(self):
        stream = ContextStream(key="b",type=int)
        with self.assertRaises(TypeError):
            stream.register(new_value=1.2)

    def test_register(self):
        stream = ContextStream(key="b",type=float)
        self.assertFalse(stream.has_value())
        stream.register(new_value=1.2)
        self.assertTrue(stream.has_value())
        self.assertEqual(stream.get(),1.2)

    def test_invalid_get_with_actions(self):
        stream = ContextStream(key="b")
        stream.register(new_value=[{"a": "result", "b": 1},2])
        actions = [ListAction(value=1),DictAction(value="a")]
        with self.assertRaises(TypeError):  
            stream.get(actions=actions) 
        
    def test_get_with_actions(self):
        stream = ContextStream(key="b")
        stream.register(new_value=[{"a": "result", "b": 1},2])
        actions = [ListAction(value=0),DictAction(value="a")]
        self.assertEqual(stream.get(actions=actions),"result")    
    
    def test_from_output_stream(self):
        stream: ContextStream = ContextStream.from_output_stream(OutputStream(key="b",type=int))
        self.assertEqual(stream.key,"b")
        self.assertEqual(stream.type,int)
