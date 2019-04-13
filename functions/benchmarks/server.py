#!/usr/bin/env python3.6

import logging
import sys
import zmq

from . import composition
from . import locality
from . import lambda_locality
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

        resp_addr = splits[0]
        bname = splits[1]
        num_requests = int(splits[2])
        if len(splits) > 3:
            create = bool(splits[3])
        else:
            create = False

        sckt = ctx.socket(zmq.PUSH)
        sckt.connect('tcp://' + resp_addr + ':3000')
        run_bench(bname, num_requests, flconn, kvs, sckt, create)

def run_bench(bname, num_requests, flconn, kvs, sckt, create=False):
    logging.info('Running benchmark %s, %d requests.' % (bname, num_requests))

    if bname == 'composition':
        total, scheduler, kvs, retries = composition.run(flconn, kvs,
                num_requests, sckt)
    elif bname == 'locality':
        total, scheduler, kvs, retries = locality.run(flconn, kvs,
                num_requests, create, sckt)
    elif bname == 'redis' or bname == 's3':
        total, scheduler, kvs, retries = lambda_locality.run(bname, kvs,
                num_requests, sckt)
    else:
        logging.info('Unknown benchmark type: %s!' % (bname))
        sckt.send(b'END')
        return

    # some benchmark modes return no results
    if not total:
        sckt.send(b'END')
        logging.info('*** Benchmark %s finished. It returned no results. ***' %
                (bname))
        return
    else:
        sckt.send(b'END')
        logging.info('*** Benchmark %s finished. ***' % (bname))

    logging.info('Total computation time: %.4f' % (sum(total)))
    if len(total) > 0:
        utils.print_latency_stats(total, 'E2E', True)
    if len(scheduler) > 0:
        utils.print_latency_stats(scheduler, 'SCHEDULER', True)
    if len(kvs) > 0:
        utils.print_latency_stats(kvs, 'KVS', True)
    logging.info('Number of KVS get retries: %d' % (retries))
