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

from anna.ipc_client import IpcAnnaClient
from anna.zmq_util import SocketCache
from .call import *
from .pin import *
from . import utils
import logging
from include.server_utils import *
from include.shared import *
import os
import time
import zmq

REPORT_THRESH = 30
global_util = 0.0

def executor(ip, mgmt_ip, schedulers, thread_id):
    global_util = 0
    logging.basicConfig(filename='log_executor.txt', level=logging.INFO)

    ctx = zmq.Context(1)
    poller = zmq.Poller()

    pin_socket = ctx.socket(zmq.REP)
    pin_socket.bind(BIND_ADDR_TEMPLATE % (PIN_PORT + thread_id))

    unpin_socket = ctx.socket(zmq.REP)
    unpin_socket.bind(BIND_ADDR_TEMPLATE % (UNPIN_PORT + thread_id))

    exec_socket = ctx.socket(zmq.REP)
    exec_socket.bind(BIND_ADDR_TEMPLATE % (FUNC_EXEC_PORT + thread_id))

    dag_queue_socket = ctx.socket(zmq.REP)
    dag_queue_socket.bind(BIND_ADDR_TEMPLATE % (DAG_QUEUE_PORT + thread_id))

    dag_exec_socket = ctx.socket(zmq.PULL)
    dag_exec_socket.bind(BIND_ADDR_TEMPLATE % (DAG_EXEC_PORT + thread_id))

    pusher_cache = SocketCache(ctx, zmq.PUSH)

    poller = zmq.Poller()
    poller.register(pin_socket, zmq.POLLIN)
    poller.register(unpin_socket, zmq.POLLIN)
    poller.register(exec_socket, zmq.POLLIN)
    poller.register(dag_queue_socket, zmq.POLLIN)
    poller.register(dag_exec_socket, zmq.POLLIN)

    client = IpcAnnaClient()

    status = ThreadStatus()
    status.ip = ip
    status.tid = thread_id
    utils._push_status(schedulers, pusher_cache, status)

    # this is going to be a map of map of maps for every function that we have
    # pinnned, we will track a map of execution ids to DAG schedules
    queue = {}

    # track the actual function objects that we are storing here
    pinned_functions = {}

    report_start = time.time()

    while True:
        socks = dict(poller.poll(timeout=1000))

        if pin_socket in socks and socks[pin_socket] == zmq.POLLIN:
            pin(pin_socket, client, status, pinned_functions)
            utils._push_status(schedulers, pusher_cache, status)

        if unpin_socket in socks and socks[unpin_socket] == zmq.POLLIN:
            unpin(unpin, status, pinned_functions)
            utils._push_status(schedulers, pusher_cache, status)

        if exec_socket in socks and socks[exec_socket] == zmq.POLLIN:
            exec_function(exec_socket, client, status)

        if dag_queue_socket in socks and socks[dag_queue_socket] == zmq.POLLIN:
            logging.info('Attempting to receive a message on the DAG queue socket.')
            schedule = DagSchedule()
            schedule.ParseFromString(dag_queue_socket.recv())
            fname = schedule.target_function

            logging.info('Received a schedule for DAG %s, function %s.' %
                    (schedule.dag.name, fname))

            # if we are trying to unpin this function, we don't accept requests
            # anymore for DAG schedules; this also checks to make sure it's the
            # right IP for the target
            if (fname not in status.functions and \
                    fname in queue and \
                    schedule.id not in queue[fname].keys()) or \
                    schedule.locations[fname].split(':')[0] != ip:
                error.error = INVALID_TARGET
                dag_queue_socket.send(error.SerializeToString())
                continue

            if fname not in queue:
                queue[fname] = {}

            queue[fname][schedule.id] = schedule
            dag_queue_socket.send(ok_resp)

        if dag_exec_socket in socks and socks[dag_exec_socket] == zmq.POLLIN:
            trigger = DagTrigger()
            trigger.ParseFromString(dag_exec_socket.recv())

            fname = trigger.target_function

            exec_dag_function(pusher_cache, client, trigger,
                    pinned_functions[fname], queue[fname][trigger.id])


        # periodically report function occupancy
        report_end = time.time()
        if report_end - report_start > REPORT_THRESH:
            util = global_util / (report_end - report_start)

            sckt = pusher_cache.get(utils._get_util_report_address(mgmt_ip))
            sckt.send_string(ip + '|' + str(util))

            logging.info('Sending utilization of %.2f%%.' % (util * 100))

            report_start = time.time()
            global_util = 0

            # periodically clear any old data we have cached
            for fname in queue:
                if len(queue[fname]) == 0:
                    del queue[fname]
                    del pinned_functions[fname]

