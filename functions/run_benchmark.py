#!/usr/bin/env python3

import logging
import sys
from benchmarks import composition
from benchmarks import dist_avg
from benchmarks import locality
from benchmarks import predserving
from benchmarks import scaling
from benchmarks import summa
from benchmarks import utils
import client as flclient

logging.basicConfig(stream=sys.stdout, level=logging.INFO)

if len(sys.argv) < 4:
    print('Usage: ./run_benchmark.py benchmark_name function_elb num_requests '
          + '{ip}')
    sys.exit(1)

f_elb = sys.argv[2]
num_requests = int(sys.argv[3])

if len(sys.argv) == 5:
    ip = sys.argv[4]
    flconn = flclient.FluentConnection(f_elb, ip)
else:
    flconn = flclient.FluentConnection(f_elb)

kvs = flconn.kvs_client

bname = sys.argv[1]

if bname == 'composition':
    total, scheduler, kvs, retries = composition.run(flconn, kvs, num_requests,
                                                     None)
elif bname == 'locality':
    locality.run(flconn, kvs, num_requests, True, None)
    total, scheduler, kvs, retries = locality.run(flconn, kvs, num_requests,
                                                  False, None)
elif bname == 'pred_serving':
    total, scheduler, kvs, retries = predserving.run(flconn, kvs,
                                                     num_requests, None)
elif bname == 'avg':
    total, scheduler, kvs, retries = dist_avg.run(flconn, kvs, num_requests,
                                                  None)
elif bname == 'summa':
    total, scheduler, kvs, retries = summa.run(flconn, kvs, num_requests, None)
elif bname == 'scaling':
    total, scheduler, kvs, retries = scaling.run(flconn, kvs, num_requests,
                                                 None)
else:
    print('Unknown benchmark type: %s!' % (bname))

print('Total computation time: %.4f' % (sum(total)))

if total:
    utils.print_latency_stats(total, 'E2E')
if scheduler:
    utils.print_latency_stats(scheduler, 'SCHEDULER')
if kvs:
    utils.print_latency_stats(kvs, 'KVS')

if retries > 0:
    print('Number of KVS get retries: %d' % (retries))
