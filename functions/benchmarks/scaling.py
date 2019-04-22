import cloudpickle as cp
import logging
import sys
import time

from include.functions_pb2 import *
from include.serializer import *
from . import utils

def run(flconn, kvs, num_requests, sckt, create):
    ### DEFINE AND REGISTER FUNCTIONS ###
    dag_name = 'scaling'

    if create:
        def slp(fluent, x):
            import time
            time.sleep(.050)
            return x

        cloud_sleep = flconn.register(slp, 'sleep')

        if cloud_sleep:
            print('Successfully registered sleep function.')
        else:
            sys.exit(1)

        ### TEST REGISTERED FUNCTIONS ###
        sleep_test = cloud_sleep(2).get()
        if sleep_test != 2:
            print('Unexpected result from sleep(2): %s' % (str(incr_test)))
            sys.exit(1)
        print('Successfully tested functions!')

        ### CREATE DAG ###

        functions = ['sleep']
        success, error = flconn.register_dag(dag_name, functions, [])

        if not success:
            print('Failed to register DAG: %s' % (ErrorType.Name(error)))
            sys.exit(1)

        return [], [], [], 0
    ### RUN DAG ###
    else:
        arg_map = { 'sleep' : [1] }

        total_time = []
        epoch_req_count = 0
        epoch_latencies = []

        epoch_start = time.time()
        epoch =  0
        for _ in range(num_requests):
            start = time.time()
            res = flconn.call_dag(dag_name, arg_map, True)
            end = time.time()

            if res is not None:
                epoch_req_count += 1

            total_time += [end - start]
            epoch_latencies += [end - start]

            epoch_end = time.time()
            if epoch_end - epoch_start > 10:
                if sckt:
                    sckt.send(cp.dumps((epoch_req_count, epoch_latencies)))

                logging.info('EPOCH %d THROUGHPUT: %.2f' % (epoch, epoch_req_count /
                    10))
                utils.print_latency_stats(epoch_latencies, 'EPOCH %d E2E' % epoch,
                    True)
                epoch += 1

                epoch_req_count = 0
                epoch_latencies.clear()
                epoch_start = time.time()


        return total_time, [], [], 0

