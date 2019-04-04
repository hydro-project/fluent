import logging
import numpy as np
import random
import scipy.stats
import sys
import time
import uuid

from anna.lattices import *
from include.functions_pb2 import *
from include.kvs_pb2 import *
from include.serializer import *
from include.shared import *

logging.basicConfig(filename='log_benchmark.txt', level=logging.INFO)

def run(flconn, kvs, num_requests):
    ### DEFINE AND REGISTER FUNCTIONS ###
    def all_mean(arr):
        return arr.mean(axis=0).mean()

    cloud_mean = flconn.register(all_mean, 'mean')

    if cloud_mean:
        print('Successfully registered the mean function.')
    else:
        sys.exit(1)

    ### TEST REGISTERED FUNCTIONS ###
    inp = np.zeros((2048, 2048))
    val = LWWPairLattice(0, serialize_val(inp))
    key = str(uuid.uuid4())
    kvs.put(key, val)

    ref = FluentReference(key, True, LWW)
    mean_test = cloud_mean(ref).get()
    if mean_test != 0.0:
        print('Unexpected result from mean(array): %s' % (str(incr_test)))

    print('Successfully tested function!')

    ### CREATE DAG ###

    dag_name = 'locality'

    functions = ['mean']
    connections = []
    success, error = flconn.register_dag(dag_name, functions, connections)

    if not success:
        print('Failed to register DAG: %s' % (ErrorType.Name(error)))
        sys.exit(1)

    ### GENERATE_DATA ###

    NUM_OBJECTS = 100
    oids = []

    for _ in range(NUM_OBJECTS):
        array = np.random.rand(2048, 2048)
        oid = str(uuid.uuid4())
        val = LWWPairLattice(0, serialize_val(array))

        kvs.put(oid, val)
        oids.append(oid)

    ### RUN DAG ###

    total_time = []
    scheduler_time = []
    kvs_time = []

    retries = 0

    log_start = time.time()

    def mean_confidence_interval(data, confidence=0.95):
        n = len(data)
        m, se = np.mean(data), scipy.stats.sem(data)
        h = se * scipy.stats.t.ppf((1 + confidence) / 2., n-1)
        return m, m-h, m+h

    log_epoch = 0
    for _ in range(num_requests):
        start = time.time()
        oid = random.choice(oids)
        ref = FluentReference(oid, True, LWW)
        arg_map = { 'mean' : [ref] }

        rid = flconn.call_dag(dag_name, arg_map)
        end = time.time()

        stime = end - start

        start = time.time()
        res = kvs.get(rid)
        while not res:
            retries += 1
            res = kvs.get(rid)
        res = deserialize_val(res.reveal()[1])
        end = time.time()

        ktime = end - start

        total_time += [stime + ktime]
        scheduler_time += [stime]
        kvs_time += [ktime]

        log_end = time.time()
        if (log_end - log_start) > 5:
            total_latency = np.array(total_time)
            sched_latency = np.array(scheduler_time)
            kvs_latency = np.array(kvs_time)

            interval = mean_confidence_interval(total_latency)
            logging.info('Epoch %d e2e latency: mean %.6f (%.6f, %.6f)' %
                    (log_epoch, interval[0], interval[1], interval[2]))
            interval = mean_confidence_interval(sched_latency)
            logging.info('Epoch %d scheduler latency: mean %.6f (%.6f, %.6f)' %
                    (log_epoch, interval[0], interval[1], interval[2]))
            interval = mean_confidence_interval(kvs_latency)
            logging.info('Epoch %d kvs latency: mean %.6f (%.6f, %.6f)' %
                    (log_epoch, interval[0], interval[1], interval[2]))

            log_epoch += 1
            log_start = time.time()

    return total_time, scheduler_time, kvs_time, retries

