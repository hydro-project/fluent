import zmq
from anna.client import *
from anna.lattices import *
from include.functions_pb2 import *
from include.serializer import *
from include.shared import *
import sys
import time
import uuid
import cloudpickle as cp
import os

ctx = zmq.Context(1)

r_elb = 'a27402691517511e980e40ad644e7fc2-1402796664.us-east-1.elb.amazonaws.com'
f_elb = 'a9d4fb15f517811e980e40ad644e7fc2-18905292.us-east-1.elb.amazonaws.com'

client = AnnaClient(r_elb, '3.92.88.44')

# create and register the preprocess function
def preprocess(inp):
    from skimage import filters
    import numpy as np
    return filters.gaussian(inp).reshape(1, 3, 224, 224)

sckt = ctx.socket(zmq.REQ)
sckt.connect('tcp://' + f_elb + ':' + str(FUNC_CREATE_PORT))

f = Function()
f.name = 'preprocess'
f.body = function_ser.dump(preprocess)

sckt.send(f.SerializeToString())

r = GenericResponse()
r.ParseFromString(sckt.recv())

print('preprocess created: ' + str(r.success))

# create and register the squeezenet function
def squeezenet(inp):
    import torch, torchvision
    model = torchvision.models.squeezenet1_1()
    return model(torch.tensor(inp.astype(np.float32))).detach().numpy()

f.name = 'squeezenet'
f.body = function_ser.dump(squeezenet)

sckt.send(f.SerializeToString())
r.ParseFromString(sckt.recv())

print('squeezenet created: ' + str(r.success))

# create and register the average function

def average(inputs):
    import numpy as np
    return np.mean(inputs, axis=0)

f.name = 'average'
f.body = function_ser.dump(average)

sckt.send(f.SerializeToString())
r.ParseFromString(sckt.recv())

print('average created: ' + str(r.success))

# create and register the DAG
sckt = ctx.socket(zmq.REQ)
sckt.connect('tcp://' + f_elb + ':' + str(DAG_CREATE_PORT))

dag = Dag()
dag.name = 'test'
dag.functions.append('preprocess')
dag.functions.append('squeezenet')
dag.functions.append('average')

conn = dag.connections.add()
conn.source = 'preprocess'
conn.sink = 'squeezenet'

conn = dag.connections.add()
conn.source = 'squeezenet'
conn.sink = 'average'

sckt.send(dag.SerializeToString())

r = GenericResponse()
r.ParseFromString(sckt.recv())

print('DAG test created: ' + str(r.success))

if not os.path.isfile('uuids'):
    # generate a bunch of inputs
    uuids = []

    count = int(sys.argv[1])

    start = time.time()
    for _ in range(count):
        uid = str(uuid.uuid4())
        arr = np.random.randn(1, 224, 224, 3)
        l = LWWPairLattice(generate_timestamp(0), serialize_val(arr))
        client.put(uid, l)

        uuids.append(uid)
    end = time.time()

    with open('uuids', 'wb') as f:
        cp.dump(uuids, f)

    print('Finished generating data (%.4fs)' % (end - start))
else:
    with open('uuids', 'rb') as f:
        uuids = cp.load(f)

# call the DAG we just created
dc = DagCall()
dc.name = 'test'

al = dc.function_args['preprocess']

sckt = ctx.socket(zmq.REQ)
sckt.connect('tcp://' + f_elb + ':' + str(DAG_CALL_PORT))

retry_count = 0
sched_time = 0
get_time = 0

tot_start = time.time()
count = 0
for uid in uuids:
    al.ClearField('args')
    ref = FluentReference(uid, True, LWW)
    al.args.extend([serialize_val(ref, None, False)])

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

    count += 1

tot_end = time.time()

print()
print('Total number of requests: %d' % (count))
print('Total elapsed time: %.2f' % (tot_end - tot_start))
print('Total computation time: %.2f' % (sched_time + get_time))
print('Average latency: %.4f' % ((sched_time + get_time) / count))
print()
print('Average scheduler time: %.4f' % (sched_time / count))
print('Average get time: %.4f' % (get_time / count))
print('Total number of retried gets: %d' % (retry_count))
