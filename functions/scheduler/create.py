#  Copyright 2018 U.C. Berkeley RISE Lab
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.

import logging
import random
import sys
import zmq

from anna.lattices import *
from include.functions_pb2 import *
import include.server_utils as sutils
from include.shared import *
from . import utils

def create_func(func_create_socket, kvs):
    func = Function()
    func.ParseFromString(func_create_socket.recv())

    name = sutils._get_func_kvs_name(func.name)
    logging.info('Creating function %s.' % (name))

    body = LWWPairLattice(generate_timestamp(0), func.body)
    kvs.put(name, body)

    funcs = utils._get_func_list(kvs, '', fullname=True)
    funcs.append(name)
    utils._put_func_list(kvs, funcs)

    func_create_socket.send(sutils.ok_resp)


def create_dag(dag_create_socket, pusher_cache, kvs, executors, dags,
        func_locations, call_frequency, num_replicas=15):
    serialized = dag_create_socket.recv()

    dag = Dag()
    dag.ParseFromString(serialized)
    logging.info('Creating DAG %s.' % (dag.name))

    payload = LWWPairLattice(generate_timestamp(0), serialized)
    kvs.put(dag.name, payload)

    for fname in dag.functions:
        candidates = set(executors)
        for _ in range(num_replicas):
            if len(candidates) == 0:
                break

            node, tid = random.sample(candidates, 1)[0]

            # this is currently a fire-and-forget operation -- we can see if we
            # want to make stronger guarantees in the future
            sckt = pusher_cache.get(utils._get_pin_address(node, tid))
            sckt.send_string(fname)

            if fname not in call_frequency:
                call_frequency[fname] = 0

            if fname not in func_locations:
                func_locations[fname] = set()

            func_locations[fname].add((node, tid))
            candidates.remove((node, tid))

    dags[dag.name] = (dag, utils._find_dag_source(dag))
    dag_create_socket.send(sutils.ok_resp)

