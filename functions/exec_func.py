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
from executor.call import *
from executor.pin import *
import logging
from include.server_utils import *
from include.shared import *
import os
from sched_func import scheduler
import time
import zmq

REPORT_THRESH = 30
global_util = 0.0

def run():
    global global_util
    mgmt_ip = os.environ['MGMT_IP']

    sys_func = os.environ['SYSTEM_FUNC']
    if sys_func == 'scheduler':
        route_addr = os.environ['ROUTE_ADDR']
        scheduler(mgmt_ip, route_addr)

    logging.basicConfig(filename='log.txt', level=logging.INFO)

    ctx = zmq.Context(1)
    poller = zmq.Poller()

    ip = os.environ['MY_IP']
    schedulers = os.environ['SCHED_IPS']
    thread_id = int(os.environ['THREAD_ID'])

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

    poller = zmq.Poller()
    poller.register(pin_socket, zmq.POLLIN)
    poller.register(unpin_socket, zmq.POLLIN)
    poller.register(exec_socket, zmq.POLLIN)

    client = IpcAnnaClient()

    status = ThreadStatus()
    status.ip = ip
    status.tid = thread_id

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

        if unpin_socket in socks and socks[unpin_socket] == zmq.POLLIN:
            unpin(unpin, status, pinned_functions)

        if exec_socket in socks and socks[exec_socket] == zmq.POLLIN:
            exec_function(exec_socket, client, status, error)

        if dag_queue_socket in socks and socks[dag_queue_socket] == zmq.POLLIN:
            schedule = DagSchedule()
            schedule.ParseFromString(dag_queue_socket.recv())
            fname = schedule.target_function

            # if we are trying to unpin this function, we don't accept requests
            # anymore for DAG schedules; this also checks to make sure it's the
            # right IP for the target
            if schedule.id not in queue[fname].keys() or \
                    status.functions[fname] == CLEARING or \
                    schedule.locations[fname] != ip:
                error.error = INVALID_TARGET
                dag_queue_socket.send(error.SerializeToString())
                continue

            queue[fname][schedule.id] = schedule
            dag_queue_socket.send_string(ok_resp)

        if dag_exec_socket in socks and socks[dag_exec_socket] == zmq.POLLIN:
            trigger = DagTrigger()
            trigger.ParseFromString(dag_exec_socket.recv())

            exec_dag_function(ctx, client, trigger,
                    pinned_functions[trigger.name],
                    queue[trigger.name][trigger.id])


        # periodically report function occupancy
        report_end = time.time()
        if report_end - report_start > REPORT_THRESH:
            util = global_util / (report_end - report_start)

            sckt = ctx.socket(zmq.PUSH)
            sckt.connect('tcp://' + mgmt_ip + ':7003')
            sckt.send_string(ip + '|' + str(util))

            logging.info('Sending utilization of %.2f%%.' % (util * 100))

            report_start = time.time()
            global_util = 0



if __name__ == '__main__':
    run()
