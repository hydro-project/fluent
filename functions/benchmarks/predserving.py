import cloudpickle as cp
import logging
import sys
import time
import uuid

from anna.lattices import *
from include.functions_pb2 import *
from include.kvs_pb2 import *
from include.shared import *
from include.serializer import *

def run(flconn, kvs, num_requests, sckt):
    ### DEFINE AND REGISTER FUNCTIONS ###

    def preprocess(fluent, inp):
        from skimage import filters
        return filters.gaussian(inp).reshape(1, 3, 224, 224)

    def sqnet(fluent, inp):
        import torch, torchvision

        model = torchvision.models.squeezenet1_1()
        return model(torch.tensor(inp.astype(np.float32))).detach().numpy()

    def average(fluent, inp1, inp2, inp3):
        import numpy as np
        inp = [inp1, inp2, inp3]
        return np.mean(inp, axis=0)

    cloud_prep = flconn.register(preprocess, 'preprocess')
    cloud_sqnet1 = flconn.register(sqnet, 'sqnet1')
    cloud_sqnet2 = flconn.register(sqnet, 'sqnet2')
    cloud_sqnet3 = flconn.register(sqnet, 'sqnet3')
    cloud_average = flconn.register(average, 'average')

    if cloud_prep and cloud_sqnet1 and cloud_sqnet2 and cloud_sqnet3 and \
            cloud_average:
        print('Successfully registered preprocess, sqnet, and average functions.')
    else:
        sys.exit(1)

    ### TEST REGISTERED FUNCTIONS ###
    arr = np.random.randn(1, 224, 224, 3)
    prep_test = cloud_prep(arr).get()
    if type(prep_test) != np.ndarray:
        print('Unexpected result from preprocess(arr): %s' % (str(prep_test)))
        sys.exit(1)

    sqnet_test1 = cloud_sqnet1(prep_test).get()
    if type(prep_test) != np.ndarray:
        print('Unexpected result from squeezenet1(arr): %s' % (str(sqnet_test)))
        sys.exit(1)

    sqnet_test2 = cloud_sqnet2(prep_test).get()
    if type(prep_test) != np.ndarray:
        print('Unexpected result from squeezenet2(arr): %s' % (str(sqnet_test)))
        sys.exit(1)

    sqnet_test3 = cloud_sqnet3(prep_test).get()
    if type(prep_test) != np.ndarray:
        print('Unexpected result from squeezenet3(arr): %s' % (str(sqnet_test)))
        sys.exit(1)

    average_test = cloud_average(sqnet_test1, sqnet_test2, sqnet_test3).get()
    if type(prep_test) != np.ndarray:
        print('Unexpected result from squeezenet(arr): %s' % (str(sqnet_test)))
        sys.exit(1)

    print('Successfully tested functions!')

    ### CREATE DAG ###

    dag_name = 'pred_serving'

    functions = ['preprocess', 'sqnet1', 'sqnet2', 'sqnet3', 'average']
    connections = [('preprocess', 'sqnet1'), ('preprocess', 'sqnet2'),
            ('preprocess', 'sqnet3'), ('sqnet1', 'average'), ('sqnet2',
                'average'), ('sqnet3', 'average')]
    success, error = flconn.register_dag(dag_name, functions, connections)

    if not success:
        print('Failed to register DAG: %s' % (str(error)))
        sys.exit(1)

    ### RUN DAG ###
    total_time = []

    # Create all the input data
    oids = []
    for _ in range(num_requests):
        arr = np.random.randn(1, 224, 224, 3)
        val = LWWPairLattice(0, serialize_val(arr))
        oid = str(uuid.uuid4())

        kvs.put(oid, val)
        oids.append(oid)

    for i in range(num_requests):
        oid = oids[i]

        arg_map = { 'preprocess' : [FluentReference(oid, True, LWW)] }

        start = time.time()
        rid = flconn.call_dag(dag_name, arg_map, True)
        end = time.time()

        total_time += [end - start]

    if sckt:
        sckt.send(cp.dumps(total_time))
    return total_time, [], [], 0

