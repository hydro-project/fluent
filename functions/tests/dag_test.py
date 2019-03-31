import zmq
from anna.client import *
from anna.lattices import *
from include.functions_pb2 import *
from include.serializer import *
from include.shared import *
import sys
import time

ctx = zmq.Context(1)

r_elb = 'a27402691517511e980e40ad644e7fc2-1402796664.us-east-1.elb.amazonaws.com'
f_elb = 'a9d4fb15f517811e980e40ad644e7fc2-18905292.us-east-1.elb.amazonaws.com'

client = AnnaClient(r_elb, '3.92.88.44')

# create and register the incr function
def incr(x):
    return x + 1

sckt = ctx.socket(zmq.REQ)
sckt.connect('tcp://' + f_elb + ':' + str(FUNC_CREATE_PORT))

f = Function()
f.name = 'incr'
f.body = function_ser.dump(incr)

sckt.send(f.SerializeToString())

r = GenericResponse()
r.ParseFromString(sckt.recv())

print('incr created: ' + str(r.success))

# create and register the square function
def square(x):
    return x * x

f.name = 'square'
f.body = function_ser.dump(square)

sckt.send(f.SerializeToString())
r.ParseFromString(sckt.recv())

print('square created: ' + str(r.success))

# create and register the DAG
sckt = ctx.socket(zmq.REQ)
sckt.connect('tcp://' + f_elb + ':' + str(DAG_CREATE_PORT))

dag = Dag()
dag.name = 'test'
dag.functions.append('incr')
dag.functions.append('square')

conn = dag.connections.add()
conn.source = 'incr'
conn.sink = 'square'

sckt.send(dag.SerializeToString())

r = GenericResponse()
r.ParseFromString(sckt.recv())

print('DAG test created: ' + str(r.success))

# call the DAG we just created
dc = DagCall()
dc.name = 'test'

val = Value()
val.type = DEFAULT
val.body = default_ser.dump(1)

al = dc.function_args['incr']
al.args.extend([val])

sckt = ctx.socket(zmq.REQ)
sckt.connect('tcp://' + f_elb + ':' + str(DAG_CALL_PORT))

count = int(sys.argv[1])

retry_count = 0
sched_time = 0
get_time = 0

tot_start = time.time()
for  _ in range(count):
    start = time.time()
    sckt.send(dc.SerializeToString())

    r = GenericResponse()
    r.ParseFromString(sckt.recv())
    end = time.time()

    sched_time += (end - start)

    start = time.time()
    l = client.get(r.response_id)

    while l is None:
        l = client.get(r.response_id)
        retry_count += 1
    result = deserialize_val(l.reveal()[1])
    end = time.time()
    get_time += (end - start)

tot_end = time.time()

print('Total elapsed time: %.2f' % (tot_end - tot_start))
print('Total computation time: %.2f' % (sched_time + get_time))
print('Average latency: %.4f' % ((sched_time + get_time) / count))
print()
print('Average scheduler time: %.4f' % (sched_time / count))
print('Average get time: %.4f' % (get_time / count))
print('Total number of retried gets: %d' % (retry_count))
