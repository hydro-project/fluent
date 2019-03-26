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
import sys

from anna.lattices import *
from include.server_utils import *
from utils import *

def create_func(func_create_socket, kvs):
    func = Function()
    func.ParseFromString(func_create_socket.recv())

    name = _get_func_kvs_name(func.name)
    print('Creating function %s.' % (name))

    body = LWWPairLattice(generate_timestamp(0), func.body)
    kvs.put(name, body)

    funcs = _get_func_list(client, '', fullname=True)
    funcs.append(name)
    _put_func_list(client, funcs)

    func_create_socket.send(ok_resp)


def create_dag(dag_create_socket, kvs, executors):
    serialized = dag_create_socket.recv()

    dag = Dag()
    dag.ParseFromString(serialized)

    kvs.put(dag.name, serialized)

    available_nodes = current_ips.Difference(pinned_nodes)

    dag_pin_map[dag.name] = {}
    for fname in dag.functions:
        node = random.choice(available_nodes)

        sckt = ctx.socket(zmq.REQ)
        sckt.connect(_get_pin_address(node))
        sckt.send_string(fname)

        resp = sckt.recv_string()

        # figure out how to deal with the error -- we don't really want
        # to deal with error checking in detail rn?
        if "Error" in resp:
            continue

        success = True
        pinned_nodes.append(node)
        dag_pin_map[dag.name][fname] = [node]

    dags[dag.name] = (dag, _find_dag_source(dag)

def _find_dag_source(dag):
    sinks = {}
    for conn in dag.connections:
        sinks.insert(conn.sink)

    funcs = list(dag.functions)
    for sink in sinks:
        funcs.remove(sink)

    return funcs
