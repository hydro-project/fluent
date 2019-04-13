#!/usr/bin/env python3

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

from functools import reduce
import logging
import math
import os
import random
import time
import zmq

from functions_pb2 import *
from kvs_pb2 import *
from metadata_pb2 import *
import util

REPORT_PERIOD = 15
UTILIZATION_MAX = .30
PINNED_COUNT_MAX = 15
UTILIZATION_MIN = .10

LATENCY_RATIO = 1.25
CALL_COUNT_THRESHOLD = 200

GRACE_PERIOD = 180
grace_start = 0

EXECUTOR_REPORT_PERIOD = 20

NUM_EXEC_THREADS = 3
EXECUTOR_INCREASE = 2 # the number of exec nodes to add at once

logging.basicConfig(filename='log_management.txt', level=logging.INFO)

def run():
    context = zmq.Context(1)

    restart_pull_socket = context.socket(zmq.REP)
    restart_pull_socket.bind('tcp://*:7000')

    churn_pull_socket = context.socket(zmq.PULL)
    churn_pull_socket.bind('tcp://*:7001')

    list_executors_socket = context.socket(zmq.REP)
    list_executors_socket.bind('tcp://*:7002')

    function_status_socket = context.socket(zmq.PULL)
    function_status_socket.bind('tcp://*:7003')

    list_schedulers_socket = context.socket(zmq.REP)
    list_schedulers_socket.bind('tcp://*:7004')

    executor_depart_socket = context.socket(zmq.PULL)
    executor_depart_socket.bind('tcp://*:7005')

    executor_statistics_socket = context.socket(zmq.PULL)
    executor_statistics_socket.bind('tcp://*:7006')

    poller = zmq.Poller()
    poller.register(restart_pull_socket, zmq.POLLIN)
    poller.register(churn_pull_socket, zmq.POLLIN)
    poller.register(function_status_socket, zmq.POLLIN)
    poller.register(list_executors_socket, zmq.POLLIN)
    poller.register(list_schedulers_socket, zmq.POLLIN)
    poller.register(executor_depart_socket, zmq.POLLIN)
    poller.register(executor_statistics_socket, zmq.POLLIN)

    add_push_socket = context.socket(zmq.PUSH)
    add_push_socket.connect('ipc:///tmp/node_add')

    remove_push_socket = context.socket(zmq.PUSH)
    remove_push_socket.connect('ipc:///tmp/node_remove')

    # waits until the kubecfg file gets copied into the pod -- this might be
    # brittle if we try to move to a non-Ubuntu setting, but I'm not worried
    # about that for now
    while not os.path.isfile('/root/.kube/config'):
        pass

    client = util.init_k8s()

    # track the self-reported status of each function execution thread
    executor_statuses = {}
    departing_executors = {}
    function_frequencies = {}
    function_runtimes = {}
    latency_history = {}

    start = time.time()
    while True:
        socks = dict(poller.poll(timeout=1000))

        if churn_pull_socket in socks and socks[churn_pull_socket] == \
                zmq.POLLIN:
            msg = churn_pull_socket.recv_string()
            args = msg.split(':')

            if args[0] == 'add':
                logging.info('Received message: %s.' % (msg))
                msg = args[2] + ':' + args[1]
                add_push_socket.send_string(msg)
            elif args[0] == 'remove':
                msg = args[2] + ':' + args[1]
                remove_push_socket.send_string(msg)

        if restart_pull_socket in socks and socks[restart_pull_socket] == \
                zmq.POLLIN:
            msg = restart_pull_socket.recv_string()
            args = msg.split(':')

            ip = args[1]
            pod = util.get_pod_from_ip(client, ip)

            count = str(pod.status.container_statuses[0].restart_count)

            logging.info('Returning restart count %s for IP %s.' % (count, ip))
            restart_pull_socket.send_string(count)

        if list_executors_socket in socks and socks[list_executors_socket] == \
                zmq.POLLIN:
            # it doesn't matter what is in this message
            msg = list_executors_socket.recv()

            ks = KeySet()
            for ip in util.get_pod_ips(client, 'role=function'):
                ks.keys.append(ip)

            list_executors_socket.send(ks.SerializeToString())

        if function_status_socket in socks and \
                socks[function_status_socket] == zmq.POLLIN:
            status = ThreadStatus()
            status.ParseFromString(function_status_socket.recv())

            key = (status.ip, status.tid)

            # if this executor is one of the ones that's currently departing,
            # we can just ignore its status updates since we don't want
            # utilization to be skewed downwards
            if key in departing_executors:
                continue

            executor_statuses[key] = status
            logging.info(('Received thread status update from %s:%d: %.4f ' +
                    'occupancy, %d functions pinned') % (status.ip, status.tid,
                        status.utilization, len(status.functions)))

        if list_schedulers_socket in socks and socks[list_schedulers_socket] == \
                zmq.POLLIN:
            # It doesn't matter what is in this message
            msg = list_schedulers_socket.recv_string()

            ks = KeySet()
            for ip in util.get_pod_ips(client, 'role=scheduler'):
                ks.keys.append(ip)

            list_schedulers_socket.send(ks.SerializeToString())

        if executor_depart_socket in socks and \
                socks[executor_depart_socket] == zmq.POLLIN:
            ip = executor_depart_socket.recv_string()
            departing_executors[ip] -= 1

            # wait until all the executors on this IP have cleared their queues
            # and left; then we remove the node
            if departing_executors[ip] == 0:
                msg = 'function:' + ip
                remove_push_socket.send_string(msg)
                del departing_executors[ip]

        if executor_statistics_socket in socks and \
                socks[executor_statistics_socket] == zmq.POLLIN:
            stats = ExecutorStatistics()
            stats.ParseFromString(executor_statistics_socket.recv())

            for fstats in stats.statistics:
                fname = fstats.fname

                if fname not in function_frequencies:
                    function_frequencies[fname] = 0

                if fname not in function_runtimes:
                    function_runtimes[fname] = (0.0, 0)

                if fstats.HasField('runtime'):
                    old_latency = function_runtimes[fname]
                    function_runtimes[fname] = (old_latency[0] + fstats.runtime,
                            old_latency[1] + fstats.call_count)
                else:
                    function_frequencies[fname] += fstats.call_count

        end = time.time()
        if end - start > REPORT_PERIOD:
            logging.info('Checking hash ring...')
            check_hash_ring(client, context)

            logging.info('Checking for extra nodes...')
            check_unused_nodes(client, add_push_socket)

            check_executor_utilization(client, context, executor_statuses,
                    departing_executors, add_push_socket)

            check_function_load(context, function_frequencies, function_runtimes,
                    executor_statuses, latency_history)

            function_runtimes.clear()
            function_frequencies.clear()
            start = time.time()

def check_function_load(context, function_frequencies, function_runtimes,
        executor_statuses, latency_history):

    # construct a reverse index that tracks where each function is currently
    # replicated
    func_locations = {}
    for key in executor_statuses:
        status = executor_statuses[key]
        for fname in status.functions:
            if fname not in func_locations:
                func_locations[fname] = set()

            func_locations[fname].add(key)

    executors = set(executor_statuses.keys())

    for fname in function_frequencies:
        runtime = function_runtimes[fname]
        call_count = function_frequencies[fname]

        if call_count == 0 or runtime[0] == 0:
            continue

        avg_latency = runtime[0] / runtime[1]

        num_replicas = len(func_locations[fname])
        thruput = float(num_replicas * EXECUTOR_REPORT_PERIOD) * (1 / avg_latency)

        logging.info(('Function %s: %d calls, %.4f average latency, %.2f' +
                ' thruput, %d replicas.') % (fname, call_count, avg_latency,
                    thruput, num_replicas))

        if call_count > thruput * .8:
            logging.info(('Function %s: %d calls in recent period exceeds'
                + ' threshold. Adding replicas.') % (fname, call_count))
            increase = math.ceil(call_count / thruput) -  num_replicas + 1
            replicate_function(fname, context, increase, func_locations,
                    executors)
        elif fname in latency_history:
            historical, count = latency_history[fname]
            logging.info('Function %s: %.4f historical latency.' %
                    (fname, historical))

            ratio = avg_latency / historical
            if ratio > LATENCY_RATIO:
                logging.info(('Function %s: recent latency average (%.4f) is ' +
                        '%.2f times the historical average. Adding replicas.')
                        % (fname, avg_latency, ratio))
                num_replicas = math.ceil(ratio) - len(func_locations[fname]) + 1
                replicate_function(fname, context, num_replicas, func_locations,
                        executors)
            else:
                for status in executor_statuses.values():
                    if status.utilization > .9:
                        logging.info(('Node %s:%d has over 90%% utilization.'
                            + ' Replicating its functions.') % (status.ip,
                                status.tid))
                        for fname in status.functions:
                            replicate_function(fname, context, 2,
                                    func_locations, executors)

            # update these variables based on history, so we can insert them
            # into the history tracker
            rt = runtime[0] + historical * count
            hist_count = runtime[1] + count
            avg_latency = rt / hist_count
            runtime = (rt, hist_count)

        latency_history[fname] = (avg_latency, runtime[1])
        function_frequencies[fname] = 0
        function_runtimes[fname] = 0.0

def replicate_function(fname, context, num_replicas, func_locations, executors):
    if num_replicas < 0:
        return

    for _ in range(num_replicas):
        existing_replicas = func_locations[fname]
        candiate_nodes = executors.difference(existing_replicas)

        if len(candiate_nodes) == 0:
            continue

        ip, tid = random.sample(candiate_nodes, 1)[0]

        sckt = context.socket(zmq.PUSH)
        sckt.connect(util._get_executor_pin_address(ip, tid))
        sckt.send_string(fname)

        func_locations[fname].add((ip, tid))

def check_executor_utilization(client, ctx, executor_statuses,
        departing_executors, add_push_socket):
    global grace_start

    utilization_sum = 0.0
    pinned_function_count = 0

    # if no executors have joined yet, we don't need to do any reporting
    if len(executor_statuses) == 0:
        return

    for status in executor_statuses.values():
        utilization_sum += status.utilization
        pinned_function_count += len(status.functions)

    avg_utilization = utilization_sum / len(executor_statuses)
    avg_pinned_count = pinned_function_count / len(executor_statuses)
    logging.info('Average executor utilization: %.4f' % (avg_utilization))
    logging.info('Average pinned function count: %.2f' % (avg_pinned_count))

    # we check to see if the grace period has ended; we only check to implement
    # some of the elasticity decisions if that is the case
    if time.time() > (grace_start + GRACE_PERIOD):
        grace_start = 0

        if avg_utilization > UTILIZATION_MAX or avg_pinned_count > \
                PINNED_COUNT_MAX:
            logging.info(('Average utilization is %.4f. Adding %d nodes to '
                 + ' cluster.') % (avg_utilization, EXECUTOR_INCREASE))

            msg = 'function:' + str(EXECUTOR_INCREASE)
            add_push_socket.send_string(msg)

            # start the grace period after adding nodes
            grace_start = time.time()

        # we only decide to kill nodes if they are underutilized and if there
        # are at least 5 executors in the system -- we never scale down past
        # that
        num_nodes = len(executor_statuses) / NUM_EXEC_THREADS

        if avg_utilization < UTILIZATION_MIN and num_nodes > 15:
            ip = random.choice(list(executor_statuses.values())).ip
            logging.info(('Average utilization is %.4f, and there are %d '
                    + 'executors. Removing IP %s.') % (avg_utilization,
                        len(executor_statuses), ip))

            for tid in range(NUM_EXEC_THREADS):
                sckt = ctx.socket(zmq.PUSH)
                sckt.connect(util._get_executor_depart_address(ip, tid))

                if (ip, tid) in executor_statuses:
                    del executor_statuses[(ip, tid)]

                # this message does not matter
                sckt.send(b'')

            departing_executors[ip] = NUM_EXEC_THREADS


def check_hash_ring(client, context):
    route_ips = util.get_pod_ips(client, 'role=routing')

    # if there are no routing nodes in the system currently, the system is
    # still starting, so we do nothing
    if not route_ips:
        return

    ip = random.choice(route_ips)

    # TODO: move all of these into a shared location?
    route_addr_port = 6350
    storage_depart_port = 6050
    route_depart_port = 6400
    mon_depart_port = 6600

    # bind to routing port
    socket = context.socket(zmq.REQ)
    socket.connect('tcp://' + ip + ':' + str(route_addr_port))

    # get all nodes that are members
    socket.send_string('')
    resp = socket.recv()

    tm = TierMembership()
    tm.ParseFromString(resp)
    tier_data = tm.tiers

    if len(tier_data) == 0:
        return
    elif len(tier_data) > 1:
        mem_tier, ebs_tier = tier_data[0], tier_data[1] if tier_data[0].tier_id \
                == 1 else tier_data[1], tier_data[0]
    else:
        mem_tier, ebs_tier = tier_data[0], None

    # check memory tier
    mem_ips = util.get_pod_ips(client, 'role=memory')

    departed = []
    for node in mem_tier.servers:
        if node.private_ip not in mem_ips:
            departed.append(('0', node))

    # check EBS tier
    ebs_ips = []
    if ebs_tier:
        ebs_ips = util.get_pod_ips(client, 'role=ebs')
        for node in ebs_tier.servers:
            if node.private_ip not in ebs_ips:
                ebs_departed.append(('1', node))

    mon_ips = util.get_pod_ips(client, 'role=monitoring')
    storage_ips = mem_ips + ebs_ips

    logging.info('Found %d departed nodes.' % (len(departed)))
    for pair in departed:
        logging.info('Informing cluster that node %s/%s has departed.' %
                (pair[1].public_ip, pair[1].private_ip))
        msg = pair[0] + ':' + pair[1].public_ip + ':' + pair[1].private_ip
        for ip in storage_ips:
            send_msg(msg, context, ip, storage_depart_port)

        msg = 'depart:' + msg
        for ip in route_ips:
            send_msg(msg, context, ip, route_depart_port)

        for ip in mon_ips:
            send_msg(msg, context, ip, mon_depart_port)

def send_msg(msg, context, ip, port):
    sckt = context.socket(zmq.PUSH)
    sckt.connect('tcp://' + ip + ':' + str(port))
    sckt.send_string(msg)

def check_unused_nodes(client, add_push_socket):
    kinds = ['ebs', 'memory']

    for kind in kinds:
        selector = 'role=' + kind

        nodes = {}
        for node in client.list_node(label_selector=selector).items:
            ip = list(filter(lambda address: address.type == 'InternalIP',
                    node.status.addresses))[0].address
            nodes[ip] = node

        node_ips = nodes.keys()

        pod_ips = util.get_pod_ips(client, selector)
        unallocated = set(node_ips) - set(pod_ips)

        mon_ips = util.get_pod_ips(client, 'role=monitoring')
        route_ips = util.get_pod_ips(client, 'role=routing')

        logging.info('Found %d unallocated %s nodes.' % (len(unallocated),
            kind))

        if len(unallocated) > 0:
            msg = kind + ':' + str(len(unallocated))
            add_push_socket.send_string(msg)

if __name__ == '__main__':
    # wait for this file to appear before starting
    while not os.path.isfile('/fluent/setup_complete'):
        pass

    run()
