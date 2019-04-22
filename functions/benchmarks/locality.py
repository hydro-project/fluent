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

sys_random = random.SystemRandom()
OSIZE = 10

def run(flconn, kvs, num_requests, create, sckt):
    dag_name = 'locality'

    if create:
        ### DEFINE AND REGISTER FUNCTIONS ###
        def dot(fluent, v1, v2, v3, v4, v5, v6, v7, v8, v9, v10):
            import numpy as np
            s1 = np.add(v1, v2)
            s2 = np.add(v3, v4)
            s3 = np.add(v5, v6)
            s4 = np.add(v7, v8)
            s5 = np.add(v9, v10)

            s1 = np.add(s1, s2)
            s2 = np.add(s3, s4)

            s1 = np.add(s1, s2)
            s1 = np.add(s1, s5)

            return np.average(s1)

        cloud_dot = flconn.register(dot, 'dot')

        if cloud_dot:
            logging.info('Successfully registered the dot function.')
        else:
            sys.exit(1)

        ### TEST REGISTERED FUNCTIONS ###
        refs = ()
        for _ in range(10):
            inp = np.zeros(OSIZE)
            v = LWWPairLattice(0, serialize_val(inp))
            k = str(uuid.uuid4())
            kvs.put(k, v)

            refs += (FluentReference(k, True, LWW),)

        dot_test = cloud_dot(*refs).get()
        if dot_test != 0.0:
            logging.error('Unexpected result from dot(v1, v2): %s' % (str(dot_test)))
            print('Unexpected result from dot(v1, v2): %s' % (str(dot_test)))
            sys.exit(1)

        logging.info('Successfully tested function!')

        ### CREATE DAG ###

        functions = ['dot']
        connections = []
        success, error = flconn.register_dag(dag_name, functions, connections)

        if not success:
            rint('Failed to register DAG: %s' % (ErrorType.Name(error)))
            sys.exit(1)

    else:
        ### RUN DAG ###

        num_data_objects = num_requests * 10 # for the cold version
        # num_data_objects = 1 # for the hot version

        oids = []
        for i in range(num_data_objects):
            array = np.random.rand(OSIZE)
            oid = str(uuid.uuid4())
            val = LWWPairLattice(0, serialize_val(array))

            kvs.put(oid, val)
            oids.append(oid)

        total_time = []
        scheduler_time = []
        kvs_time = []

        retries = 0

        log_start = time.time()

        log_epoch = 0
        epoch_total = []

        for i in range(num_requests):
            refs = []
            for ref in oids[(i * 10):(i * 10) + 10]: # for the cold version
                refs.append(FluentReference(ref, True, LWW))
            # for _ in range(10): # for the hot version
            #     refs.appends(oids[0])

            start = time.time()
            arg_map = { 'dot' : refs }

            resp = flconn.call_dag(dag_name, arg_map, True)
            end = time.time()

            epoch_total += [end - start]

            log_end = time.time()
            if (log_end - log_start) > 10:
                if sckt:
                    sckt.send(cp.dumps(epoch_total))
                utils.print_latency_stats(epoch_total, 'EPOCH %d E2E' %
                        (log_epoch), True)

                epoch_total.clear()
                log_epoch += 1
                log_start = time.time()

        return total_time, scheduler_time, kvs_time, retries

