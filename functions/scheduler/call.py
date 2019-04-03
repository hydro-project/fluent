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

def call_function(func_call_socket, requestor_cache, executors, key_ip_map):
    call = FunctionCall()
    call.ParseFromString(func_call_socket.recv())

    logging.info('Calling function %s.' % (call.name))

    refs = list(filter(lambda arg: type(arg) == FluentReference,
        map(lambda arg: get_serializer(arg.type).load(arg.body),
            call.args)))

    ip, tid = _pick_node(executors, key_ip_map, refs)
    sckt = requestor_cache.get(utils._get_exec_address(ip, tid))
    sckt.send(call.SerializeToString())

    # we currently don't do any checking here because either the function is
    # not found (so there's nothing for the scheduler to do) or the request was
    # accepted... we could make this async in the future, or there could be
    # other error checks one might want to do.
    r = sckt.recv()
    func_call_socket.send(r)


def call_dag(call, requestor_cache, pusher_cache, dags, func_locations,
        key_ip_map):
    logging.info('Calling DAG %s.' % (call.name))

    dag, sources = dags[call.name]
    chosen_locations = {}
    for f in dag.functions:
        locations = func_locations[f]
        args = call.function_args[f].args

        refs = list(filter(lambda arg: type(arg) == FluentReference,
            map(lambda arg: get_serializer(arg.type).load(arg.body),
                args)))
        loc = _pick_node(locations, key_ip_map, refs)
        chosen_locations[f] = (loc[0], loc[1])

    schedule = DagSchedule()
    schedule.id = str(uuid.uuid4())
    schedule.dag.CopyFrom(dag)

    # copy over arguments into the dag schedule
    for fname in call.function_args:
        arg_list = schedule.arguments[fname]
        arg_list.args.extend(call.function_args[fname].args)

    for func in chosen_locations:
        loc = chosen_locations[func]
        schedule.locations[func] = loc[0] + ':' + str(loc[1])


    for func in chosen_locations:
        loc = chosen_locations[func]
        ip = utils._get_queue_address(loc[0], loc[1])
        schedule.target_function = func

        triggers = sutils._get_dag_predecessors(dag, func)
        if len(triggers) == 0:
            triggers.append('BEGIN')

        schedule.ClearField('triggers')
        schedule.triggers.extend(triggers)

        sckt = requestor_cache.get(ip)
        sckt.send(schedule.SerializeToString())

        response = GenericResponse()
        response.ParseFromString(sckt.recv())

        if not response.success:
            logging.info('Schedule operation for %s at %s failed.' % (func, ip))
            return response.success, response.error, None

    for source in sources:
        trigger = DagTrigger()
        trigger.id = schedule.id
        trigger.source = 'BEGIN'
        trigger.target_function = source

        ip = sutils._get_dag_trigger_address(schedule.locations[source])
        sckt = pusher_cache.get(ip)
        sckt.send(trigger.SerializeToString())

    return True, None, schedule.id


def _pick_node(executors, key_ip_map, refs):
    # Construct a map which maps from IP addresses to the number of
    # relevant arguments they have cached. For the time begin, we will
    # just pick the machine that has the most number of keys cached.
    arg_map = {}
    for ref in refs:
        if ref.key in key_ip_map:
            ips = key_ip_map[ref.key]

            for ip in ips:
                if ip not in arg_map:
                    arg_map[ip] = 0

                arg_map[ip] += 1

    max_ip = None
    max_count = 0
    for ip in arg_map.keys():
        if arg_map[ip] > max_count:
            max_count = arg_map[ip]
            max_ip = ip

    # pick a random thrad on that IP address
    if max_ip:
        max_ip = (max_ip, random.choice(list(range(utils.NUM_EXEC_THREADS))))

    # This only happens if max_ip is never set, and that means that
    # there were no machines with any of the keys cached. In this case,
    # we pick a random IP that was in the set of IPs that was running
    # most recently.
    if not max_ip:
        max_ip = random.sample(executors, 1)[0]

    return max_ip

