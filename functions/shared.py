import cloudpickle as cp
import codecs
from functions_pb2 import *
from io import BytesIO
import numpy as np

# shared constants
FUNCOBJ = 'funcs/index-allfuncs'
FUNC_PREFIX = 'funcs/'
BIND_ADDR_TEMPLATE = 'tcp://*:%d'

CONNECT_PORT = 4999
CREATE_PORT = 5000
CALL_PORT = 5001
LIST_PORT = 5002

SER_FORMAT = 'raw_unicode_escape'

class SkyFuture():
    def __init__(self, obj_id, kvs_client):
        self.obj_id = obj_id
        self.kvs_client = kvs_client

    def get(self):
        obj = self.kvs_client.get(self.obj_id)

        while not obj:
            obj = self.kvs_client.get(self.obj_id)

        retval = Value()
        retval.ParseFromString(obj)

        return get_serializer(retval.type).load(retval.body)

class SkyFunc():
    def __init__(self, name, conn, kvs_client):
        self.name = name
        self._conn = conn
        self._kvs_client = kvs_client

    def __call__(self, *args):
        obj_id = self._conn.exec_func(self.name, args)
        return SkyFuture(obj_id, self._kvs_client)

class SkyReference():
    def __init__(self, key, deserialize):
        self.key = key
        self.deserialize = deserialize

class Serializer():
    def __init__(self):
        raise NotImplementedError('Cannot instantiate abstract class.')

    def _serialize(self, msg):
        pass

    def _deserialize(self, msg):
        pass

    def dump(self, msg):
        pass

    def load(self, msg):
        pass

class DefaultSerializer(Serializer):
    def __init__(self):
        pass

    def _serialize(msg):
        return msg

    def _deserialize(self, msg):
        return msg

    def dump(self, msg):
        return cp.dumps(msg)

    def load(self, msg):
        return cp.loads(msg)

class StringSerializer(Serializer):
    def __init__(self):
        pass

    def _serialize(self, msg):
        return codecs.decode(msg, SER_FORMAT)

    def _deserialize(self, msg):
        return codecs.encode(msg, SER_FORMAT)

    def dump(self, msg):
        return serialize(cp.dumps(msg))

    def load(self, msg):
        return cp.loads(deserialize(msg))

# TODO: how can we make serializers pluggable?
class NumpySerializer(DefaultSerializer):
    def __init__(self):
        pass

    def dump(self, msg):
        body = BytesIO()

        np.save(body, msg)
        return body.getvalue()

    def load(self, msg):
        return np.load(BytesIO(msg))

numpy_ser = NumpySerializer()
default_ser = DefaultSerializer()
string_ser = StringSerializer()

def get_serializer(kind):
    global numpy_ser, default_ser, string_ser

    if kind == NUMPY:
        return numpy_ser
    elif kind == STRING:
        return string_ser
    elif kind == DEFAULT:
        return default_ser
    else:
        return default_ser

def serialize_val(val, valobj=None):
    if not valobj:
        valobj = Value()

    if isinstance(val, SkyFuture):
        valobj.body = default_ser.dump(SkyReference(val.obj_id, True))
    elif isinstance(val, np.ndarray):
        valobj.body = numpy_ser.dump(val)
        valobj.type = NUMPY
    else:
        valobj.body = default_ser.dump(val)

    return valobj

