#!/usr/bin/env python3.6

import numpy as np
import scipy.stats
import sys

from benchmarks import composition
import client as flclient

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
    total, scheduler, kvs, retries = composition.run(flconn, kvs, num_requests)
else:
    print('Unknown benchmark type: %s!' % (bname))

total = np.array(total)
scheduler = np.array(scheduler)
kvs = np.array(kvs)

def mean_confidence_interval(data, confidence=0.95):
    n = len(data)
    m, se = np.mean(data), scipy.stats.sem(data)
    h = se * scipy.stats.t.ppf((1 + confidence) / 2., n-1)
    return m, m-h, m+h

interval = mean_confidence_interval(total)

print('Total computation time: %.4f' % (total.sum()))
print('Average latency: %.4f (%.4f, %.4f)' % (interval[0], interval[1],
    interval[2]))

print()

interval = mean_confidence_interval(scheduler)
print('Average scheduler latency: %.4f (%.4f, %.4f)' % (interval[0], interval[1],
    interval[2]))
interval = mean_confidence_interval(kvs)
print('Average KVS get latency: %.4f (%.4f, %.4f)' % (interval[0], interval[1],
    interval[2]))
print('Number of KVS get retries: %d' % (retries))
