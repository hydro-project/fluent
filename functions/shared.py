import cloudpickle as cp
import codecs

# shared constants
FUNCOBJ = 'funcs/index-allfuncs'
FUNC_PREFIX = 'funcs/'
BIND_ADDR_TEMPLATE = 'tcp://*:%d'

MSG_PORT = 5000

SER_FORMAT = 'raw_unicode_escape'

def serialize(msg):
    return codecs.decode(msg, SER_FORMAT)

def deserialize(msg):
    return codecs.encode(msg, SER_FORMAT)

def dump(msg):
    return serialize(cp.dumps(msg))

def load(msg):
    return cp.loads(deserialize(msg))
