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
import os
import time
import zmq

from anna.ipc_client import IpcAnnaClient
from anna.zmq_util import SocketCache
from .call import *
from .pin import *
from . import utils
from include import server_utils as sutils
from include.shared import *

REPORT_THRESH = 20

def executor(ip, mgmt_ip, schedulers, thread_id):
    logging.basicConfig(filename='log_executor.txt', level=logging.INFO, format='%(asctime)s %(message)s')

    ctx = zmq.Context(1)
    poller = zmq.Poller()

    pin_socket = ctx.socket(zmq.PULL)
    pin_socket.bind(sutils.BIND_ADDR_TEMPLATE % (sutils.PIN_PORT + thread_id))

    unpin_socket = ctx.socket(zmq.PULL)
    unpin_socket.bind(sutils.BIND_ADDR_TEMPLATE % (sutils.UNPIN_PORT +
        thread_id))

    exec_socket = ctx.socket(zmq.PULL)
    exec_socket.bind(sutils.BIND_ADDR_TEMPLATE % (sutils.FUNC_EXEC_PORT +
        thread_id))

    dag_queue_socket = ctx.socket(zmq.PULL)
    dag_queue_socket.bind(sutils.BIND_ADDR_TEMPLATE % (sutils.DAG_QUEUE_PORT
        + thread_id))

    dag_exec_socket = ctx.socket(zmq.PULL)
    dag_exec_socket.bind(sutils.BIND_ADDR_TEMPLATE % (sutils.DAG_EXEC_PORT
        + thread_id))

    self_depart_socket = ctx.socket(zmq.PULL)
    self_depart_socket.bind(sutils.BIND_ADDR_TEMPLATE %
            (sutils.SELF_DEPART_PORT + thread_id))

    pusher_cache = SocketCache(ctx, zmq.PUSH)

    poller = zmq.Poller()
    poller.register(pin_socket, zmq.POLLIN)
    poller.register(unpin_socket, zmq.POLLIN)
    poller.register(exec_socket, zmq.POLLIN)
    poller.register(dag_queue_socket, zmq.POLLIN)
    poller.register(dag_exec_socket, zmq.POLLIN)
    poller.register(self_depart_socket, zmq.POLLIN)

    client = IpcAnnaClient(thread_id)

    status = ThreadStatus()
    status.ip = ip
    status.tid = thread_id
    status.running = True
    utils._push_status(schedulers, pusher_cache, status)

    departing = False

    # this is going to be a map of map of maps for every function that we have
    # pinnned, we will track a map of execution ids to DAG schedules
    queue = {}

    # track the actual function objects that we are storing here
    pinned_functions = {}

    # tracks runtime cost of excuting a DAG function
    runtimes = {}

    # if multiple triggers are necessary for a function, track the triggers as
    # we receive them
    received_triggers = {}

    # track when we received a function request, so we can report e2e latency
    receive_times = {}

    # track how many functions we're executing
    exec_counts = {}

    # metadata to track thread utilization
    report_start = time.time()
    event_occupancy = { 'pin': 0.0, 'unpin': 0.0, 'func_exec': 0.0,
            'dag_queue': 0.0, 'dag_exec': 0.0 }
    total_occupancy = 0.0

    while True:
        socks = dict(poller.poll(timeout=1000))

        if pin_socket in socks and socks[pin_socket] == zmq.POLLIN:
            work_start = time.time()
            pin(pin_socket, client, status, pinned_functions, runtimes,
                    exec_counts)
            utils._push_status(schedulers, pusher_cache, status)

            elapsed = time.time() - work_start
            event_occupancy['pin'] += elapsed
            total_occupancy += elapsed

        if unpin_socket in socks and socks[unpin_socket] == zmq.POLLIN:
            work_start = time.time()
            unpin(unpin_socket, status, pinned_functions, runtimes, exec_counts)
            utils._push_status(schedulers, pusher_cache, status)

            elapsed = time.time() - work_start
            event_occupancy['unpin'] += elapsed
            total_occupancy += elapsed

        if exec_socket in socks and socks[exec_socket] == zmq.POLLIN:
            work_start = time.time()
            exec_function(exec_socket, client, status)

            elapsed = time.time() - work_start
            event_occupancy['func_exec'] += elapsed
            total_occupancy += elapsed

        if dag_queue_socket in socks and socks[dag_queue_socket] == zmq.POLLIN:
            work_start = time.time()

            schedule = DagSchedule()
            schedule.ParseFromString(dag_queue_socket.recv())
            fname = schedule.target_function

            logging.info('Received a schedule for DAG %s (%s), function %s.' %
                    (schedule.dag.name, schedule.id, fname))

            if fname not in queue:
                queue[fname] = {}

            queue[fname][schedule.id] = schedule

            if (schedule.id, fname) not in receive_times:
                receive_times[(schedule.id, fname)] = time.time()

            # in case we receive the trigger before we receive the schedule, we
            # can trigger from this operation as well
            trkey = (schedule.id, fname)
            if trkey in received_triggers and \
                    len(received_triggers[trkey]) == \
                            len(schedule.triggers):
                exec_dag_function(pusher_cache, client,
                    received_triggers[trkey], pinned_functions[fname],
                    schedule)
                del received_triggers[trkey]
                del queue[fname][schedule.id]

                fend = time.time()
                fstart = receive_times[(schedule.id, fname)]
                runtimes[fname] += fend - fstart
                exec_counts[fname] += 1

            elapsed = time.time() - work_start
            event_occupancy['dag_queue'] += elapsed
            total_occupancy += elapsed

        if dag_exec_socket in socks and socks[dag_exec_socket] == zmq.POLLIN:
            work_start = time.time()
            trigger = DagTrigger()
            trigger.ParseFromString(dag_exec_socket.recv())

            fname = trigger.target_function
            logging.info('Received a trigger for schedule %s, function %s.' %
                    (trigger.id, fname))

            key = (trigger.id, fname)
            if trigger.id not in received_triggers:
                received_triggers[key] = {}

            if (trigger.id, fname) not in receive_times:
                receive_times[(trigger.id, fname)] = time.time()

            received_triggers[key][trigger.source] = trigger
            if fname in queue and trigger.id in queue[fname]:
                schedule = queue[fname][trigger.id]
                if len(received_triggers[key]) == len(schedule.triggers):
                    exec_dag_function(pusher_cache, client,
                            received_triggers[key],
                            pinned_functions[fname], schedule)
                    del received_triggers[key]
                    del queue[fname][trigger.id]

                    fend = time.time()
                    fstart = receive_times[(trigger.id, fname)]
                    runtimes[fname] += fend - fstart
                    exec_counts[fname] += 1

            elapsed = time.time() - work_start
            event_occupancy['dag_exec'] += elapsed
            total_occupancy += elapsed

        if self_depart_socket in socks and socks[self_depart_socket] == \
                zmq.POLLIN:
            # This message should not matter
            msg = self_depart_socket.recv()

            logging.info('Preparing to depart. No longer accepting requests ' +
                    'and clearing all queues.')

            status.ClearField('functions')
            status.running = False
            utils._push_status(schedulers, pusher_cache, status)

            departing = True

        # periodically report function occupancy
        report_end = time.time()
        if report_end - report_start > REPORT_THRESH:
            utilization = total_occupancy / (report_end - report_start)
            status.utilization = utilization

            sckt = pusher_cache.get(utils._get_util_report_address(mgmt_ip))
            sckt.send(status.SerializeToString())

            logging.info('Total thread occupancy: %.6f%%' % (utilization))

            for event in event_occupancy:
                occ = event_occupancy[event]
                logging.info('Event %s occupancy: %.6f%%' % (event, occ))
                event_occupancy[event] = 0.0

            stats = ExecutorStatistics()
            for fname in runtimes:
                if exec_counts[fname] > 0:
                    fstats = stats.statistics.add()
                    fstats.fname = fname
                    fstats.runtime = runtimes[fname]
                    fstats.call_count = exec_counts[fname]

                runtimes[fname] = 0.0
                exec_counts[fname] = 0

            sckt = pusher_cache.get(sutils._get_statistics_report_address \
                    (mgmt_ip))
            sckt.send(stats.SerializeToString())

            report_start = time.time()
            total_occupancy = 0.0

            # periodically clear any old functions we have cached that we are
            # no longer accepting requests for
            for fname in queue:
                if len(queue[fname]) == 0 and fname not in status.functions:
                    del queue[fname]
                    del pinned_functions[fname]
                    del runtimes[fname]
                    del exec_counts[fname]

            # if we are departing and have cleared our queues, let the
            # management server know, and exit the process
            if departing and len(queue) == 0:
                sckt = pusher_cache.get(utils._get_depart_done_addr(mgmt_ip))
                sckt.send_string(ip)

                return 0

