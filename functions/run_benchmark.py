#!/usr/bin/env python3.6

import logging
import sys

from benchmarks import composition
from benchmarks import locality
from benchmarks import user_library_test
from benchmarks import retwis_benchmark
from benchmarks import utils
import client as flclient

logging.basicConfig(stream=sys.stdout, level=logging.INFO)

### SETUP ###
if len(sys.argv) < 4:
    print('Usage: ./run_benchmark.py benchmark_name function_elb num_requests {ip}')
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
elif bname == 'user_library_test':
    user_library_test.run(flconn, kvs, num_requests)
elif bname == 'retwis':
    total, scheduler, kvs, retries = retwis_benchmark.run(flconn, kvs, create=False, num_requests=num_requests, count_anomalies=False, sckt=None, reply_frac=0.5)
elif bname == 'retwis-noreply':
    total, scheduler, kvs, retries = retwis_benchmark.run(flconn, kvs, create=False, num_requests=num_requests, count_anomalies=False, sckt=None, reply_frac=0.0)
elif bname == 'retwis-anom':
    total, scheduler, kvs, retries = retwis_benchmark.run(flconn, kvs, create=False, num_requests=num_requests, count_anomalies=True, sckt=None)
elif bname == 'retwis-create':
    total, scheduler, kvs, retries = retwis_benchmark.run(flconn, kvs, create=True, num_requests=0, count_anomalies=False, sckt=None)
elif bname == 'retwis-populate':
    total, scheduler, kvs, retries = retwis_benchmark.run(flconn, kvs, create=True, num_requests=num_requests, count_anomalies=False, sckt=None, reply_frac=0.5)
elif bname == 'retwis-populate-noreply':
    total, scheduler, kvs, retries = retwis_benchmark.run(flconn, kvs, create=True, num_requests=num_requests, count_anomalies=False, sckt=None, reply_frac=0.0)
else:
    print('Unknown benchmark type: %s!' % (bname))

print('Total computation time: %.4f' % (sum(total)))

utils.print_latency_stats(total, 'E2E')
utils.print_latency_stats(scheduler, 'SCHEDULER')
utils.print_latency_stats(kvs, 'KVS')

print('Number of KVS get retries: %d' % (retries))
