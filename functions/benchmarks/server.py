#!/usr/bin/env python3.6

import logging
import sys
import zmq

from . import composition
from . import locality
from . import lambda_locality
from . import causal
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

        sckt = ctx.socket(zmq.PUSH)
        sckt.connect('tcp://' + resp_addr + ':3000')
        run_bench(bname, num_requests, flconn, kvs, sckt)

def run_bench(bname, num_requests, flconn, kvs, sckt, create=False):
    logging.info('Running benchmark %s, %d requests.' % (bname, num_requests))

    if bname == 'causal':
        causal.run(flconn, kvs, num_requests, sckt)
    else:
        logging.info('Unknown benchmark type: %s!' % (bname))
        sckt.send(b'END')
        return

    # some benchmark modes return no results
    sckt.send(b'END')
    logging.info('*** Benchmark %s finished. ***' % (bname))
