import boto3
import cloudpickle as cp
import json
import logging
import random
from rediscluster import StrictRedisCluster
import time
import uuid

from . import utils

sys_random = random.SystemRandom()
def run(name, kvs, num_requests, sckt, redis_addr):
    if name == 'redis':
        redis = StrictRedisCluster(startup_nodes=[{'host': redis_addr, 'port': 6379}],
            decode_responses=False, skip_full_coverage_check=True)
    else:
        s3 = boto3.client('s3')

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
        for _ in range(2):
            args.append(sys_random.choice(oids))

        start = time.time()
        loc = str(uuid.uuid4())
        body = { 'args': args, 'loc': loc }
        lambd.invoke(FunctionName=name, Payload=json.dumps(body),
                InvocationType='Event')
        # res = json.loads(res['Payload'].read())
        # kvs, comp = res
        end = time.time()
        invoke = end - start

        start = time.time()
        if 'redis' in name:
            while not redis.exists(loc):
                time.sleep(.002)
                continue
            res = cp.loads(redis.get(loc))
        else:
            results = s3.list_objects(Bucket='vsreekanti', Prefix=loc)
            found = 'Contents' in results
            while not found:
                time.sleep(.002)
                results = s3.list_objects(Bucket='vsreekanti', Prefix=loc)
                found = 'Contents' in results

            res = cp.loads(s3.get_object(Bucket='vsreekanti',
                Key=loc)['Body'].read())
        end = time.time()

        kvs = end - start
        epoch_kvs.append(kvs)
        # epoch_comp.append(comp)

        total = invoke + kvs
        latencies.append(total)
        epoch_latencies.append(total)
        epoch_end = time.time()

        if (epoch_end - epoch_start) > 10:
            sckt.send(cp.dumps(epoch_latencies))
            utils.print_latency_stats(epoch_latencies, 'EPOCH %d E2E' %
                    (epoch), True)
            # utils.print_latency_stats(epoch_comp, 'EPOCH %d COMP' %
            #         (epoch), True)
            utils.print_latency_stats(epoch_kvs, 'EPOCH %d KVS' %
                    (epoch), True)
            epoch += 1

            epoch_latencies.clear()
            epoch_kvs.clear()
            epoch_comp.clear()
            epoch_start = time.time()

    return latencies, [], [], 0
