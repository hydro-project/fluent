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
import uuid
import zmq

from include.functions_pb2 import *
from include.serializer import *
from include import server_utils as sutils
from include.shared import *
from . import utils

sys_random = random.SystemRandom()

def call_function(func_call_socket, pusher_cache, executors, key_ip_map,
        running_counts, backoff):
    call = FunctionCall()
    call.ParseFromString(func_call_socket.recv())

    if not call.HasField('resp_id'):
        call.resp_id = str(uuid.uuid4())

    logging.info('Calling function %s.' % (call.name))

    refs = list(filter(lambda arg: type(arg) == FluentReference,
        map(lambda arg: get_serializer(arg.type).load(arg.body),
            call.args)))

    ip, tid = _pick_node(executors, key_ip_map, refs, running_counts, backoff)
    sckt = pusher_cache.get(utils._get_exec_address(ip, tid))
    sckt.send(call.SerializeToString())

    r = GenericResponse()
    r.success = True
    r.response_id = call.resp_id

    func_call_socket.send(r.SerializeToString())


def call_dag(call, pusher_cache, dags, func_locations, key_ip_map,
        running_counts, backoff):
    dag, sources = dags[call.name]

    schedule = DagSchedule()
    schedule.id = str(uuid.uuid4())
    schedule.dag.CopyFrom(dag)
    schedule.consistency = NORMAL

    logging.info('Calling DAG %s (%s).' % (call.name, schedule.id))

    for fname in dag.functions:
        locations = func_locations[fname]
        args = call.function_args[fname].args

        refs = list(filter(lambda arg: type(arg) == FluentReference,
            map(lambda arg: get_serializer(arg.type).load(arg.body),
                args)))
        loc = _pick_node(locations, key_ip_map, refs, running_counts, backoff)
        schedule.locations[fname] = loc[0] + ':' + str(loc[1])

        # copy over arguments into the dag schedule
        arg_list = schedule.arguments[fname]
        arg_list.args.extend(args)

    # logging.info('Sending %s to %s!' % (schedule.id, schedule.locations['dot']))

    for func in schedule.locations:
        loc = schedule.locations[func].split(':')
        ip = utils._get_queue_address(loc[0], loc[1])
        schedule.target_function = func

        triggers = sutils._get_dag_predecessors(dag, func)
        if len(triggers) == 0:
            triggers.append('BEGIN')

        schedule.ClearField('triggers')
        schedule.triggers.extend(triggers)

        sckt = pusher_cache.get(ip)
        sckt.send(schedule.SerializeToString())

    for source in sources:
        trigger = DagTrigger()
        trigger.id = schedule.id
        trigger.source = 'BEGIN'
        trigger.target_function = source

        ip = sutils._get_dag_trigger_address(schedule.locations[source])
        sckt = pusher_cache.get(ip)
        sckt.send(trigger.SerializeToString())

    return schedule.id


def _pick_node(valid_executors, key_ip_map, refs, running_counts, backoff):
    # Construct a map which maps from IP addresses to the number of
    # relevant arguments they have cached. For the time begin, we will
    # just pick the machine that has the most number of keys cached.
    arg_map = {}
    reason = ''

    executors = set(valid_executors)
    for executor in backoff:
        if len(executors) > 1:
            executors.discard(executor)

    keys = list(running_counts.keys())
    sys_random.shuffle(keys)
    for key in keys:
        if len(running_counts[key]) > 50 and len(executors) > 1:
            executors.discard(key)

    executor_ips = [e[0] for e in executors]

    for ref in refs:
        if ref.key in key_ip_map:
            ips = key_ip_map[ref.key]

            for ip in ips:
                # only choose this cached node if its a valid executor for our
                # purposes
                if ip in executor_ips:
                    if ip not in arg_map:
                        arg_map[ip] = 0

                    arg_map[ip] += 1

    max_ip = None
    max_count = 0
    for ip in arg_map.keys():
        # logging.info('IP %s has count %d' % (ip, arg_map[ip]))
        if arg_map[ip] > max_count:
            max_count = arg_map[ip]
            max_ip = ip

    # pick a random thead from our potential executors that is on that IP
    # address; we also route some requests to a random valid node
    if max_ip:
        candidates = list(filter(lambda e: e[0] == max_ip, executors))
        max_ip = sys_random.choice(candidates)
        reason = 'locality'

    # This only happens if max_ip is never set, and that means that
    # there were no machines with any of the keys cached. In this case,
    # we pick a random IP that was in the set of IPs that was running
    # most recently.
    if not max_ip or sys_random.random() < 0.20:
        max_ip = sys_random.sample(executors, 1)[0]
        reason = 'random'

    if max_ip not in running_counts:
        running_counts[max_ip] = set()

    running_counts[max_ip].add(time.time())
    # logging.info('Picking node %s:%d for reason %s' % (max_ip[0], max_ip[1],
    #     reason))

    return max_ip

