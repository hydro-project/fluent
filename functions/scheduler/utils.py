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

from anna.lattices import *
from include.kvs_pb2 import *
from include.shared import *
from include.serializer import *
from include.server_utils import *

FUNCOBJ = 'funcs/index-allfuncs'

NUM_EXEC_THREADS = 3

EXECUTORS_PORT = 7002
SCHEDULERS_PORT = 7004

def _get_func_list(client, prefix, fullname=False):
    funcs = client.get(FUNCOBJ)
    if not funcs:
        return []

    funcs = default_ser.load(funcs.reveal()[1])

    prefix = FUNC_PREFIX + prefix
    result = list(filter(lambda fn: fn.startswith(prefix), funcs))

    if not fullname:
        result = list(map(lambda fn: fn.split(FUNC_PREFIX)[-1], result))

    return result


def _put_func_list(client, funclist):
    # remove duplicates
    funclist = list(set(funclist))

    l = LWWPairLattice(generate_timestamp(0), default_ser.dump(funclist))
    client.put(FUNCOBJ, l)


def _get_cache_ip_key(ip):
    return 'ANNA_METADATA|cache_ip|' + ip


def _get_pin_address(ip, tid):
    return 'tcp://' + ip + ':' + str(PIN_PORT + tid)


def _get_unpin_address(ip, tid):
    return 'tcp://' + ip + ':' + str(UNPIN_PORT + tid)


def _get_exec_address(ip, tid):
    return 'tcp://' + ip + ':' + str(FUNC_EXEC_PORT + tid)


def _get_queue_address(ip, tid):
    return 'tcp://' + ip + ':' + str(DAG_QUEUE_PORT + int(tid))


def _get_scheduler_list_address(mgmt_ip):
    return 'tcp://' + mgmt_ip + ':' + str(SCHEDULERS_PORT)


def _get_executor_list_address(mgmt_ip):
    return 'tcp://' + mgmt_ip + ':' + str(EXECUTORS_PORT)


def _get_scheduler_update_address(ip):
    return 'tcp://' + ip + ':' + str(SCHED_UPDATE_PORT)


def _get_ip_set(request_ip, socket_cache, exec_threads=True):
    sckt = socket_cache.get(request_ip)

    # we can send an empty request because the response is always thes same
    sckt.send(b'')

    ips = KeySet()
    ips.ParseFromString(sckt.recv())
    result = set()

    if exec_threads:
        for ip in ips.keys:
            for i in range(NUM_EXEC_THREADS):
                result.add((ip, i))

        return result
    else:
        return set(ips.keys)


def _update_key_maps(kc_map, key_ip_map, executors, kvs):
    exec_ips = set(map(lambda e: e[0], executors))
    for ip in set(kc_map.keys()).difference(exec_ips): del kc_map[ip]

    key_ip_map.clear()
    for ip in exec_ips:
        key = _get_cache_ip_key(ip)

        # this is of type LWWPairLattice, which has a KeySet protobuf packed
        # into it; we want the keys in that KeySet protobuf
        l = kvs.get(key)
        if l is None: # this executor is still joining
            continue

        ks = KeySet()
        ks.ParseFromString(l.reveal()[1])

        kc_map[ip] = set(ks.keys)

        for key in ks.keys:
            if key not in key_ip_map:
                key_ip_map[key] = []

            key_ip_map[key].append(ip)

def _find_dag_source(dag):
    sinks = set()
    for conn in dag.connections:
        sinks.add(conn.sink)

    funcs = set(dag.functions)
    for sink in sinks:
        funcs.remove(sink)

    return funcs
