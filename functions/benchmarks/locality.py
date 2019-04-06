import cloudpickle as cp
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
from . import utils

def run(flconn, kvs, num_requests, create, sckt):
    dag_name = 'locality'

    if create:
        ### DEFINE AND REGISTER FUNCTIONS ###
        def dot(v1, v2):
            import numpy as np
            return np.dot(v1, v2)

        cloud_dot = flconn.register(dot, 'dot')

        if cloud_dot:
            logging.info('Successfully registered the dot function.')
        else:
            sys.exit(1)

        ### TEST REGISTERED FUNCTIONS ###
        inp1 = np.zeros(1024 * 1024)
        v1 = LWWPairLattice(0, serialize_val(inp1))
        k1 = str(uuid.uuid4())
        kvs.put(k1, v1)

        inp2 = np.zeros(1024 * 1024)
        v2 = LWWPairLattice(0, serialize_val(inp2))
        k2 = str(uuid.uuid4())
        kvs.put(k2, v2)

        r1 = FluentReference(k1, True, LWW)
        r2 = FluentReference(k2, True, LWW)
        dot_test = cloud_dot(r1, r2).get()
        if dot_test != 0.0:
            logging.error('Unexpected result from dot(v1, v2): %s' % (str(dot_test)))
            sys.exit(1)

        logging.info('Successfully tested function!')

        ### CREATE DAG ###

        functions = ['dot']
        connections = []
        success, error = flconn.register_dag(dag_name, functions, connections)

        if not success:
            rint('Failed to register DAG: %s' % (ErrorType.Name(error)))
            sys.exit(1)

        ### GENERATE_DATA ###

        NUM_OBJECTS = 200
        oids = []

        for _ in range(NUM_OBJECTS):
            array = np.random.rand(1024 * 1024)
            oid = str(uuid.uuid4())
            val = LWWPairLattice(0, serialize_val(array))

            kvs.put(oid, val)
            oids.append(oid)

        oid_data = cp.dumps(oids)
        l = LWWPairLattice(generate_timestamp(0), oid_data)
        kvs.put('LOCALITY_OIDS', l)
        logging.info('Successfully created all data!')

        return [], [], [], []
    else:
        ### RUN DAG ###
        l = kvs.get('LOCALITY_OIDS')
        oids = cp.loads(l.reveal()[1])

        total_time = []
        scheduler_time = []
        kvs_time = []

        retries = 0

        log_start = time.time()

        log_epoch = 0
        epoch_total = []
        epoch_scheduler = []
        epoch_kvs = []

        for _ in range(num_requests):
            start = time.time()
            oid = random.choice(oids)
            r1 = FluentReference(oid, True, LWW)
            oid = random.choice(oids)
            r2 = FluentReference(oid, True, LWW)
            arg_map = { 'dot' : [r1, r2] }

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

            epoch_total += [stime + ktime]
            epoch_scheduler += [stime]
            epoch_kvs += [ktime]

            log_end = time.time()
            if (log_end - log_start) > 5:
                sckt.send(cp.dumps(epoch_total))
                utils.print_latency_stats(epoch_total, 'EPOCH %d E2E' %
                        (log_epoch), True)
                utils.print_latency_stats(epoch_scheduler, 'EPOCH %d SCHEDULER' %
                        (log_epoch), True)
                utils.print_latency_stats(epoch_kvs, 'EPOCH %d KVS' %
                        (log_epoch), True)

                epoch_total.clear()
                epoch_scheduler.clear()
                epoch_kvs.clear()
                log_epoch += 1
                log_start = time.time()

        return total_time, scheduler_time, kvs_time, retries

