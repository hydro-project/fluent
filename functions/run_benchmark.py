#!/usr/bin/env python3.6

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
    composition.run(flconn, kvs, num_requests)
else:
    print('Unknown benchmark type: %s!' % (bname))
