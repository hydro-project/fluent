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
import time
import uuid
import zmq

from anna.client import AnnaClient
from anna.zmq_util import SocketCache
from include.kvs_pb2 import *
from include.functions_pb2 import *
from include import server_utils as sutils
from include.shared import *
from include.serializer import *
from .create import *
from .call import *
from . import utils

THRESHOLD = 5  # how often metadata is updated


def scheduler(ip, mgmt_ip, route_addr):
    logging.basicConfig(filename='log_scheduler.txt', level=logging.INFO,
                        format='%(asctime)s %(message)s')

    kvs = AnnaClient(route_addr, ip)

    key_ip_map = {}
    ctx = zmq.Context(1)

    # Each dag consists of a set of functions and connections. Each one of
    # the functions is pinned to one or more nodes, which is tracked here.
    dags = {}
    thread_statuses = {}
    func_locations = {}
    running_counts = {}
    backoff = {}

    connect_socket = ctx.socket(zmq.REP)
    connect_socket.bind(sutils.BIND_ADDR_TEMPLATE % (CONNECT_PORT))

    func_create_socket = ctx.socket(zmq.REP)
    func_create_socket.bind(sutils.BIND_ADDR_TEMPLATE % (FUNC_CREATE_PORT))

    func_call_socket = ctx.socket(zmq.REP)
    func_call_socket.bind(sutils.BIND_ADDR_TEMPLATE % (FUNC_CALL_PORT))

    dag_create_socket = ctx.socket(zmq.REP)
    dag_create_socket.bind(sutils.BIND_ADDR_TEMPLATE % (DAG_CREATE_PORT))

    dag_call_socket = ctx.socket(zmq.REP)
    dag_call_socket.bind(sutils.BIND_ADDR_TEMPLATE % (DAG_CALL_PORT))

    list_socket = ctx.socket(zmq.REP)
    list_socket.bind(sutils.BIND_ADDR_TEMPLATE % (LIST_PORT))

    exec_status_socket = ctx.socket(zmq.PULL)
    exec_status_socket.bind(sutils.BIND_ADDR_TEMPLATE % (sutils.STATUS_PORT))

    sched_update_socket = ctx.socket(zmq.PULL)
    sched_update_socket.bind(sutils.BIND_ADDR_TEMPLATE %
                             (sutils.SCHED_UPDATE_PORT))

    backoff_socket = ctx.socket(zmq.PULL)
    backoff_socket.bind(sutils.BIND_ADDR_TEMPLATE % (sutils.BACKOFF_PORT))

    pin_accept_socket = ctx.socket(zmq.PULL)
    pin_accept_socket.setsockopt(zmq.RCVTIMEO, 500)
    pin_accept_socket.bind(sutils.BIND_ADDR_TEMPLATE %
                           (sutils.PIN_ACCEPT_PORT))

    requestor_cache = SocketCache(ctx, zmq.REQ)
    pusher_cache = SocketCache(ctx, zmq.PUSH)

    poller = zmq.Poller()
    poller.register(connect_socket, zmq.POLLIN)
    poller.register(func_create_socket, zmq.POLLIN)
    poller.register(func_call_socket, zmq.POLLIN)
    poller.register(dag_create_socket, zmq.POLLIN)
    poller.register(dag_call_socket, zmq.POLLIN)
    poller.register(list_socket, zmq.POLLIN)
    poller.register(exec_status_socket, zmq.POLLIN)
    poller.register(sched_update_socket, zmq.POLLIN)
    poller.register(backoff_socket, zmq.POLLIN)

    executors = set()
    executor_status_map = {}
    schedulers = _update_cluster_state(requestor_cache, mgmt_ip, executors,
                                       key_ip_map, kvs)

    # track how often each DAG function is called
    call_frequency = {}

    start = time.time()

    while True:
        socks = dict(poller.poll(timeout=1000))

        if connect_socket in socks and socks[connect_socket] == zmq.POLLIN:
            msg = connect_socket.recv_string()
            connect_socket.send_string(route_addr)

        if (func_create_socket in socks and
                socks[func_create_socket] == zmq.POLLIN):
            create_func(func_create_socket, kvs)

        if func_call_socket in socks and socks[func_call_socket] == zmq.POLLIN:
            call_function(func_call_socket, pusher_cache, executors,
                          key_ip_map, executor_status_map, running_counts,
                          backoff)

        if (dag_create_socket in socks and socks[dag_create_socket]
                == zmq.POLLIN):
            create_dag(dag_create_socket, pusher_cache, kvs, executors, dags,
                       ip, pin_accept_socket, func_locations, call_frequency)

        if dag_call_socket in socks and socks[dag_call_socket] == zmq.POLLIN:
            call = DagCall()
            call.ParseFromString(dag_call_socket.recv())

            if call.name not in dags:
                resp = GenericResponse()
                resp.success = False
                resp.error = NO_SUCH_DAG

                dag_call_socket.send(resp.SerializeToString())
                continue

            exec_id = generate_timestamp(0)

            dag = dags[call.name]
            for fname in dag[0].functions:
                call_frequency[fname] += 1

            rid = call_dag(call, pusher_cache, dags, func_locations,
                           key_ip_map, running_counts, backoff)

            resp = GenericResponse()
            resp.success = True
            resp.response_id = rid
            dag_call_socket.send(resp.SerializeToString())

        if list_socket in socks and socks[list_socket] == zmq.POLLIN:
            logging.info('Received query for function list.')
            msg = list_socket.recv_string()
            prefix = msg if msg else ''

            resp = FunctionList()
            flist = utils._get_func_list(kvs, prefix)
            if len(flist) == 0:
                logging.info('Function list is empty.')
            resp.names.extend(flist)

            list_socket.send(resp.SerializeToString())

        if exec_status_socket in socks and socks[exec_status_socket] == \
                zmq.POLLIN:
            status = ThreadStatus()
            status.ParseFromString(exec_status_socket.recv())

            key = (status.ip, status.tid)
            logging.info('Received status update from executor %s:%d.' %
                         (key[0], int(key[1])))

            if key in executor_status_map:
                if status.type == PERIODIC:
                    if executor_status_map[key] - time.time() > 5:
                        del executor_status_map[key]
                    else:
                        continue
                elif status.type == POST_REQUEST:
                    del executor_status_map[key]

            # this means that this node is currently departing, so we remove it
            # from all of our metadata tracking
            if not status.running:
                if key in thread_statuses:
                    old_status = thread_statuses[key]
                    del thread_statuses[key]

                    for fname in old_status.functions:
                        func_locations[fname].discard((old_status.ip,
                                                       old_status.tid))

                executors.discard(key)
                continue

            if key not in executors:
                executors.add(key)

            if key in thread_statuses and thread_statuses[key] != status:
                # remove all the old function locations, and all the new ones
                # -- there will probably be a large overlap, but this shouldn't
                # be much different than calculating two different set
                # differences anyway
                for func in thread_statuses[key].functions:
                    if func in func_locations:
                        func_locations[func].discard(key)

            thread_statuses[key] = status
            for func in status.functions:
                if func not in func_locations:
                    func_locations[func] = set()

                func_locations[func].add(key)

        if sched_update_socket in socks and socks[sched_update_socket] == \
                zmq.POLLIN:
            status = SchedulerStatus()
            status.ParseFromString(sched_update_socket.recv())

            # retrieve any DAG that some other scheduler knows about that we do
            # not yet know about
            for dname in status.dags:
                if dname not in dags:
                    payload = kvs.get(dname)
                    while not payload:
                        payload = kvs.get(dname)
                    dag = Dag()
                    dag.ParseFromString(payload.reveal()[1])

                    dags[dag.name] = (dag, utils._find_dag_source(dag))

                    for fname in dag.functions:
                        if fname not in call_frequency:
                            call_frequency[fname] = 0

                        if fname not in func_locations:
                            func_locations[fname] = set()

            for floc in status.func_locations:
                key = (floc.ip, floc.tid)
                fname = floc.name

                if fname not in func_locations:
                    func_locations[fname] = set()

                func_locations[fname].add(key)

        if backoff_socket in socks and socks[backoff_socket] == zmq.POLLIN:
            msg = backoff_socket.recv_string()
            splits = msg.split(':')
            node, tid = splits[0], int(splits[1])

            backoff[(node, tid)] = time.time()

        # periodically clean up the running counts map
        for executor in running_counts:
            call_times = running_counts[executor]
            new_set = set()
            for ts in call_times:
                if time.time() - ts < 2.5:
                    new_set.add(ts)

            running_counts[executor] = new_set

        remove_set = set()
        for executor in backoff:
            if time.time() - backoff[executor] > 5:
                remove_set.add(executor)

        for executor in remove_set:
            del backoff[executor]

        end = time.time()
        if end - start > THRESHOLD:
            schedulers = _update_cluster_state(requestor_cache, mgmt_ip,
                                               executors, key_ip_map, kvs)

            status = SchedulerStatus()
            for name in dags.keys():
                status.dags.append(name)

            for fname in func_locations:
                for loc in func_locations[fname]:
                    floc = status.func_locations.add()
                    floc.name = fname
                    floc.ip = loc[0]
                    floc.tid = loc[1]

            msg = status.SerializeToString()

            for sched_ip in schedulers:
                if sched_ip != ip:
                    sckt = pusher_cache.get(utils._get_scheduler_update_address
                                            (sched_ip))
                    sckt.send(msg)

            stats = ExecutorStatistics()
            for fname in call_frequency:
                fstats = stats.statistics.add()
                fstats.fname = fname
                fstats.call_count = call_frequency[fname]
                logging.info('Reporting %d calls for function %s.' %
                             (call_frequency[fname], fname))

                call_frequency[fname] = 0

            sckt = pusher_cache.get(sutils._get_statistics_report_address
                                    (mgmt_ip))
            sckt.send(stats.SerializeToString())

            start = time.time()


def _update_cluster_state(requestor_cache, mgmt_ip, executors, key_ip_map,
                          kvs):
    # update our local key-cache mapping information
    utils._update_key_maps(key_ip_map, executors, kvs)

    schedulers = utils._get_ip_set(utils._get_scheduler_list_address(mgmt_ip),
                                   requestor_cache, False)

    return schedulers
