import cloudpickle as cp
import zmq
from sched_func import *
from anna.lattices import *
from anna.client import *
from include.functions_pb2 import *
from include.serializer import *
from include.shared import *

ctx = zmq.Context(1)

r_elb = 'a80f8bbe64ff511e9b0500af8acf1c75-123615276.us-east-1.elb.amazonaws.com'
f_elb = 'a4d54183d4ff611e9b0500af8acf1c75-305570500.us-east-1.elb.amazonaws.com'

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
