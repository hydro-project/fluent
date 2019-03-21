import zmq
from include.functions_pb2 import *
from include.serializer import*

ctx = zmq.Context(1)

f_elb = '34.229.219.93'
r_elb = 'a31373ab54bec11e9a5a40a9f27cf828-1977051996.us-east-1.elb.amazonaws.com'

sckt = ctx.socket(zmq.REQ)
sckt.connect('tcp://' + f_elb + ':4002')

fc = FunctionCall()
fc.name = 'scheduler'
fc.request_id = 1

v1 = Value()
v1.body = default_ser.dump('100.96.1.5')
v1.type = STRING

v2 = Value()
v2.body = default_ser.dump(f_elb)
v2.type = STRING

fc.args.extend([v1, v2])

sckt.send(fc.SerializeToString())

print(sckt.recv())
