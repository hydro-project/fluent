import cloudpickle as cp
import zmq
from anna.lattices import *
from anna.client import *
from include.functions_pb2 import *
from include.serializer import *
from include.shared import *

ctx = zmq.Context(1)

r_elb = 'a3962fba750b311e9aae40a474700d18-157780011.us-east-1.elb.amazonaws.com'
f_elb = 'aff49b29a50b311e9aae40a474700d18-1839066821.us-east-1.elb.amazonaws.com'

client = AnnaClient(r_elb, '3.89.81.130')

sckt = ctx.socket(zmq.REQ)
sckt.connect('tcp://' + f_elb + ':' + str(FUNC_CREATE_PORT))

def incr(x):
    return x + 1

f = Function()
f.body = cp.dumps(incr)
f.name = 'incr'

sckt.send(f.SerializeToString())

r = GenericResponse()
r.ParseFromString(sckt.recv())

print(r)

fc = FunctionCall()
fc.name = 'incr'
fc.request_id = 0

v = Value()
v.body = default_ser.dump(1)
v.type = DEFAULT

fc.args.extend([v])

sckt = ctx.socket(zmq.REQ)
sckt.connect('tcp://' + f_elb + ':' + str(FUNC_CALL_PORT))
sckt.send(fc.SerializeToString())

r.ParseFromString(sckt.recv())
print(r)

l = client.get(r.response_id)

print(l is None)

if l:
    print(deserialize_val(l.reveal()[1]))
