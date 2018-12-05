import cloudpickle as cp
import codecs
import functions_pb2
from io import BytesIO

# shared constants
FUNCOBJ = 'funcs/index-allfuncs'
FUNC_PREFIX = 'funcs/'
BIND_ADDR_TEMPLATE = 'tcp://*:%d'

CONNECT_PORT = 4999
CREATE_PORT = 5000
CALL_PORT = 5001
LIST_PORT = 5002

SER_FORMAT = 'raw_unicode_escape'

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

class NumpySerializer(DefaultSerializer):
    def __init__(self):
        pass

    def dump(msg):
        body = BytesIO()

        np.save(body, msg)
        return body.getvalue()

    def load(msg):
        return np.load(BytesIO(msg))

numpy_ser = NumpySerializer()
default_ser = DefaultSerializer()
string_ser = StringSerializer()

def get_serializer(kind):
    global numpy_ser, default_ser, string_ser

    if kind == functions_pb2.NUMPY:
        return numpy_ser
    elif kind == functions_pb2.STRING:
        return string_ser
    elif kind == functions_pb2.DEFAULT:
        return default_ser
    else:
        return default_ser
