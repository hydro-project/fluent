import boto3
import cloudpickle as cp
import json
import logging
import random
import time

from . import utils

def run(name, kvs, num_requests, sckt):
    name = 'locality-' + name
    oids = cp.loads(kvs.get(name).reveal()[1])

    lambd = boto3.client('lambda', 'us-east-1')

    latencies = []
    epoch_latencies = []
    epoch_kvs = []
    epoch_comp = []
    epoch_start = time.time()

    epoch = 0
    for _ in range(num_requests):
        args = []
        for _ in range(10):
            args.append(random.choice(oids))

        start = time.time()
        body = { 'args': args }
        res = lambd.invoke(FunctionName=name, Payload=json.dumps(body))
        res = json.loads(res['Payload'].read())
        kvs, comp = res
        end = time.time()

        epoch_kvs.append(kvs)
        epoch_comp.append(comp)

        latencies.append(end - start)
        epoch_latencies.append(end - start)
        epoch_end = time.time()

        if (epoch_end - epoch_start) > 10:
            sckt.send(cp.dumps(epoch_latencies))
            utils.print_latency_stats(epoch_latencies, 'EPOCH %d E2E' %
                    (epoch), True)
            utils.print_latency_stats(epoch_comp, 'EPOCH %d COMP' %
                    (epoch), True)
            utils.print_latency_stats(epoch_kvs, 'EPOCH %d KVS' %
                    (epoch), True)
            epoch += 1

            epoch_latencies.clear()
            epoch_kvs.clear()
            epoch_comp.clear()
            epoch_start = time.time()

    return latencies, [], [], 0
