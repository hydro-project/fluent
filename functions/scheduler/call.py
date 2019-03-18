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
sys.path.append('..')

from include.function_pb2 import *
from include.misc_pb2 import *
from include.serializer import *
from utils import *

def call_function(func_call_socket, ctx, executors, key_ip_map):
    call = FunctionCall()
    call.ParseFromString(func_call_socket.recv())

    refs = list(filter(lambda arg: type(arg) == FluentReference,
        map(lambda arg: get_serializer(arg.type).load(arg.body),
            call.args)))

    ip = _pick_node(executors, key_ip_map, refs)
    sckt = ctx.socket(zmq.REQ)
    sckt.connect(_get_exec_address(ip))
    sckt.send(call.SerializeToString())

    # we currently don't do any checking here because either the function is
    # not found (so there's nothing for the scheduler to do) or the request was
    # accepted... we could make this async in the future, or there could be
    # other error checks one might want to do.
    response = GenericResponse()
    func_call_socket.send(sckt.recv())


def call_dag(call, ctx, dags, func_locations, key_ip_map, uid):
    dag, sources = dags[call.name)
    chosen_locations = {}
    for f in dag.functions:
        locations = func_locations[f]
        args = call.function_args[f]

        refs = list(filter(lambda arg: type(arg) == FluentReference,
            map(lambda arg: get_serializer(arg.type).load(arg.body),
                args)))
        ip = _pick_node(locations, key_ip_map, refs)
        chosen_locations[f] = ip

    schedule = DagSchedule()
    schedule.id = generate_timestamp(0)
    schedule.dag = dag
    schedule.arguments = call.function_args

    for func in chosen_locations:
        schedule.locations[func] = chosen_locations[func]

    for func in chosen_locations:
        ip = _get_queue_address(chosen_locations[func])
        schedule.target_function = func

        sckt = ctx.socket(zmq.REQ)
        sckt.connect(ip)
        sckt.send(schedule.SerializeToString())

        response = GenericResponse()
        response.ParseFromString(sckt.recv())

        if not response.success:
            return response.success, response.error

    for source in sources:
        trigger = DagTrigger()
        trigger.id = schedule.id
        trigger.target_function = source

        ip = _get_trigger_address(schedule.locations[source])
        sckt = ctx.socket(zmq.PUSH)
        sckt.connect(ip)
        sckt.send(trigger.SerializeToString())

    return True, None


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

                arg_map_ip += 1

    max_ip = None
    max_count = 0
    for ip in arg_map.keys():
        if arg_map[ip] > max_count:
            max_count = arg_map[ip]
            max_ip = ip

    # This only happens if max_ip is never set, and that means that
    # there were no machines with any of the keys cached. In this case,
    # we pick a random IP that was in the set of IPs that was running
    # most recently.
    if not max_ip:
        max_ip = random.choice(executors)

    return max_ip

