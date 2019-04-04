#!/usr/bin/env python3.6

import logging
import sys
import zmq

from . import composition
from . import locality
from . import utils

BENCHMARK_START_PORT = 3000

def benchmark(flconn, tid):
    logging.basicConfig(filename='log_benchmark.txt', level=logging.INFO, format='%(asctime)s %(message)s')

    ctx = zmq.Context(1)

    benchmark_start_socket = ctx.socket(zmq.PULL)
    benchmark_start_socket.bind('tcp://*:' + str(BENCHMARK_START_PORT + tid))

    kvs = flconn.kvs_client

    while True:
        msg = benchmark_start_socket.recv_string()
        splits = msg.split(':')

        bname = splits[0]
        num_requests = int(splits[1])
        if len(splits) > 2:
            create = bool(splits[2])
        else:
            create = False
        run_bench(bname, num_requests, flconn, kvs, create)


def run_bench(bname, num_requests, flconn, kvs, create=False):
    logging.info('Running benchmark %s, %d requests.' % (bname, num_requests))
    if bname == 'composition':
        total, scheduler, kvs, retries = composition.run(flconn, kvs, num_requests)
    elif bname == 'locality':
        total, scheduler, kvs, retries = locality.run(flconn, kvs,
                num_requests, create)
    else:
        logging.info('Unknown benchmark type: %s!' % (bname))
        return

    # some benchmark modes return no results
    if not total:
        return

    logging.info('*** BENCHMARK %s FINISHED ***' % (bname))
    logging.info('Total computation time: %.4f' % (sum(total)))
    utils.print_latency_stats(total, 'E2E', True)
    utils.print_latency_stats(scheduler, 'SCHEDULER', True)
    utils.print_latency_stats(kvs, 'KVS', True)
    logging.info('Number of KVS get retries: %d' % (retries))
