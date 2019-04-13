import cloudpickle as cp
import logging
import numpy as np
import random
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
        def dot(v1, v2, v3, v4, v5, v6, v7, v8, v9, v10):
            import numpy as np
            d1 = np.dot(v1, v2)
            d2 = np.dot(v3, v4)
            d3 = np.dot(v5, v6)
            d4 = np.dot(v7, v8)
            d5 = np.dot(v9, v10)
            return d1 + d2 + d3 + d4 + d5

        cloud_dot = flconn.register(dot, 'dot')

        if cloud_dot:
            logging.info('Successfully registered the dot function.')
        else:
            sys.exit(1)

        ### TEST REGISTERED FUNCTIONS ###
        refs = ()
        for _ in range(10):
            inp = np.zeros(1024*10)
            v = LWWPairLattice(0, serialize_val(inp))
            k = str(uuid.uuid4())
            kvs.put(k, v)

            refs += (FluentReference(k, True, LWW),)

        dot_test = cloud_dot(*refs).get()
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
            array = np.random.rand(1024*10)
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
            refs = []
            for _ in range(10):
                oid = random.choice(oids)
                refs.append(FluentReference(oid, True, LWW))

            arg_map = { 'dot' : refs }

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
                if sckt:
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

