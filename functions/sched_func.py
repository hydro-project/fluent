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

import random
import sys
import time
import uuid
import zmq

from anna.client import AnnaClient
from include.kvs_pb2 import *
from include.functions_pb2 import *
from include.server_utils import *
from include.shared import *
from include.serializer import *

def _get_cache_ip_key(ip):
    return 'ANNA_METADATA|cache_ip|' + ip

def _get_ip_list(mgmt_ip, port, ctx):
    print('calling get ip list')
    sckt = ctx.socket(zmq.REQ)
    sckt.connect('tcp://' + mgmt_ip + ':' + str(port))

    # we can send an empty request because the response is always thes same
    sckt.send(b'')

    ips = KeySet()
    ips.ParseFromString(sckt.recv())
    print('Retrieved news ips set:')
    print(str(ips))
    return list(ips.keys)

def _update_key_maps(kc_map, key_ip_map, executors, kvs):
    for ip in set(kc_map.keys()).difference(executors): del kc_map[ip]

    key_ip_map.clear()
    for ip in executors:
        key = get_cache_ip_key(ip)

        # this is of type LWWPairLattice, which has a KeySet protobuf packed
        # into it; we want the keys in that KeySet protobuf
        l = kvs.get(key)
        ks = KeySet()
        ks.ParseFromString(l.reveal()[1])

        kc_map[ip] = set(ks.keys())

        for key in ks.keys():
            if key not in key_ip_map:
                key_ip_map[key] = []

            key_ip_map[key].append(ip)


def scheduler(mgmt_ip, route_addr):
    print("ASDF12354987asdfa")
    import logging
    logging.basicConfig(filename='log.txt', level=logging.INFO)

    kvs = AnnaClient(route_addr)

    key_cache_map = {}
    key_ip_map = {}
    ctx = zmq.Context(1)

    # Each dag consists of a set of functions and connections. Each one of
    # the functions is pinned to one or more nodes, which is tracked here.
    dags = {}
    pinned_functions = {}
    func_locations = {}

    connect_socket = ctx.socket(zmq.REP)
    connect_socket.bind(BIND_ADDR_TEMPLATE % (CONNECT_PORT))

    func_create_socket = ctx.socket(zmq.REP)
    func_create_socket.bind(BIND_ADDR_TEMPLATE % (FUNC_CREATE_PORT))

    func_call_socket = ctx.socket(zmq.REP)
    func_call_socket.bind(BIND_ADDR_TEMPLATE % (FUNC_CALL_PORT))

    dag_create_socket = ctx.socket(zmq.REP)
    dag_create_socket.bind(BIND_ADDR_TEMPLATE % (DAG_CREATE_PORT))

    dag_call_socket = ctx.socket(zmq.REP)
    dag_call_socket.bind(BIND_ADDR_TEMPLATE % (DAG_CALL_PORT))

    list_socket = ctx.socket(zmq.REP)
    list_socket.bind(BIND_ADDR_TEMPLATE % (LIST_PORT))

    exec_status_socket = ctx.socket(zmq.PULL)
    exec_status_socket.bind(BIND_ADDR_TEMPLATE % (STATUS_PORT))

    sched_update_socket = ctx.socket(zmq.PULL)
    sched_update_socket.bind(BIND_ADDR_TEMPLATE % (SCHED_UPDATE_PORT))

    poller = zmq.Poller()
    poller.register(connect_socket, zmq.POLLIN)
    poller.register(func_create_socket, zmq.POLLIN)
    poller.register(func_call_socket, zmq.POLLIN)
    poller.register(dag_create_socket, zmq.POLLIN)
    poller.register(dag_call_socket, zmq.POLLIN)
    poller.register(list_socket, zmq.POLLIN)
    poller.register(exec_status_socket, zmq.POLLIN)
    poller.register(sched_update_socket, zmq.POLLIN)

    executors = _get_ip_list(mgmt_ip, NODES_PORT, ctx)
    update_key_maps(key_cache_map, key_ip_map, executors, kvs)
    schedulers = _get_ip_list(mgmt_ip, SCHEDULERS_PORT, ctx)

    start = time.time()

    print('Starting the while loop.')
    while True:
        socks = dict(poller.poll(timeout=1000))

        if connect_socket in socks and socks[connect_socket] == zmq.POLLIN:
            print('Received connect')
            msg = connect_socket.recv_string()
            connect_socket.send_string(routing_addr)

        if func_create_socket in socks and socks[func_create_socket] == zmq.POLLIN:
            print('Received create')
            create_func(func_create_socket, kvs)

        if func_call_socket in socks and socks[func_call_socket] == zmq.POLLIN:
            print('Received call')
            call_function(func_call_socket, ctx, executors, key_ip_map)

        if dag_create_socket in socks and socks[list_socket] == zmq.POLLIN:
            print('Received dag create')
            create_dag(dag_create_socket, kvs, executors)

        if dag_call_socket in socks and socks[list_socket] == zmq.POLLIN:
            print('Received dag call')
            call = DagCall()
            call.ParseFromString(dag_call_socket.recv())
            exec_id = generate_timestamp(0)

            accepted, error = call_dag(call, ctx, dags, func_locations,
                    key_ip_map)

            while not accepted:
                executors = _get_ip_list(mgmt_ip, NODES_PORT, ctx)
                update_key_maps(key_cache_map, key_ip_map, executors, kvs)

                accepted, error = call_dag(call, ctx, dags, func_locations,
                        key_ip_map)


        if list_socket in socks and socks[list_socket] == zmq.POLLIN:
            print('Received list')
            msg = list_socket.recv_string()
            prefix = msg if msg else ''

            resp = FunctionList()
            resp.names.append(_get_func_list(client, prefix))

            list_socket.send(resp.SerializeToString())

        if exec_status_socket in socks and socks[exec_status_socket] == \
                zmq.POLLIN:
            status = ThreadStatus()
            status.ParseFromString(exec_status_socket.recv())

            key = (status.ip, status.tid)
            if key not in executors:
                pinned_functions[key] = status
            elif pinned_functions[key] != status:
                # remove all the old function locations, and all the new ones
                # -- there will probably be a large overlap, but this shouldn't
                # be much different than calculating two different set
                # differences anyway
                for func in pinned_functions[key].functions:
                    func_locations[key].remove(key)

                for func in status.functions:
                    func_locations[key].insert(key)

                pinned_functions[key] = status

        if sched_update_socket in socks and socks[sched_update_socket] == \
                zmq.POLLIN:
            ks = KeySet()
            ks.ParseFromString(sched_update_socket.recv())

            # retrieve any DAG that some other scheduler knows about that we do
            # not yet know about
            for dname in ks:
                if dname not in dags:
                    dag = Dag()
                    dag.ParseFromString(kvs.get(dname).value)

                    dags[dname] = dag


        end = time.time()
        if start - end > THRESHOLD:
            # update our local key-cache mapping information
            executors = _get_ip_list(mgmt_ip, NODES_PORT, ctx)
            update_key_maps(key_cache_map, key_ip_map, executors, kvs)

            schedulers = _get_ip_list(mgmt_ip, SCHEDULERS_PORT, ctx)

            dag_names = KeySet()
            for name in dags.keys():
                dag_names.add_keys(name)
            msg = dag_names.SerializeToString()

            for sched_ip in schedulers:
                sckt = ctx.socket(zmq.PUSH)
                sckt.connect('tcp://' + sched_ip + ':' +
                        str(SCHED_UPDATE_PORT))
                sckt.send(msg)

