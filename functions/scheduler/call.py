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
from . import utils

def call_function(func_call_socket, ctx, executors, key_ip_map):
    call = FunctionCall()
    call.ParseFromString(func_call_socket.recv())

    logging.info('Calling function %s.' % (call.name))

    refs = list(filter(lambda arg: type(arg) == FluentReference,
        map(lambda arg: get_serializer(arg.type).load(arg.body),
            call.args)))

    ip, tid = _pick_node(executors, key_ip_map, refs)
    sckt = ctx.socket(zmq.REQ)
    sckt.connect(utils._get_exec_address(ip, tid))
    sckt.send(call.SerializeToString())

    # we currently don't do any checking here because either the function is
    # not found (so there's nothing for the scheduler to do) or the request was
    # accepted... we could make this async in the future, or there could be
    # other error checks one might want to do.
    func_call_socket.send(sckt.recv())


def call_dag(call, ctx, dags, func_locations, key_ip_map):
    logging.info('Calling DAG %s.' % (call.name))

    dag, sources = dags[call.name]
    chosen_locations = {}
    for f in dag.functions:
        locations = func_locations[f]
        logging.info('All locations are %s.' % (str(func_locations)))
        logging.info('Potential locations are %s.' % (str(locations)))
        args = call.function_args[f].args

        refs = list(filter(lambda arg: type(arg) == FluentReference,
            map(lambda arg: get_serializer(arg.type).load(arg.body),
                args)))
        loc = _pick_node(locations, key_ip_map, refs)
        chosen_locations[f] = (loc[0], loc[1])

    schedule = DagSchedule()
    schedule.id = generate_timestamp(0)
    schedule.dag.CopyFrom(dag)

    # copy over arguments into the dag schedule
    for fname in call.function_args:
        arg_list = schedule.arguments[fname]
        arg_list.args.extend(call.function_args[fname].args)

    resp_id = str(uuid.uuid4())
    schedule.response_id = resp_id

    for func in chosen_locations:
        loc = chosen_locations[func]
        schedule.locations[func] = loc[0] + ':' + str(loc[1])

    logging.info('Successfully constructed schedule!')

    for func in chosen_locations:
        loc = chosen_locations[func]
        ip = utils._get_queue_address(loc[0], loc[1])
        schedule.target_function = func

        logging.info('Sending schedule to %s.' % (ip))

        sckt = ctx.socket(zmq.REQ)
        sckt.connect(ip)
        sckt.send(schedule.SerializeToString())

        logging.info('Successfully sent the schedule. Now waiting for a response.')

        response = GenericResponse()
        response.ParseFromString(sckt.recv())

        if not response.success:
            logging.info('Pin operation for %s at %s failed.' % (func, ip))
            return response.success, response.error, None
        else:
            logging.info('Pin operation for %s at %s succeeded.' % (func, ip))

    logging.info('Attempting to trigger the following sources: %s.' %
            (str(sources)))
    for source in sources:
        trigger = DagTrigger()
        trigger.id = schedule.id
        trigger.target_function = source

        ip = sutils._get_dag_trigger_address(schedule.locations[source])
        logging.info('Sending DAG trigger to source %s at %s.' % (source, ip))
        sckt = ctx.socket(zmq.PUSH)
        sckt.connect(ip)
        sckt.send(trigger.SerializeToString())

    logging.info('Success! Response ID is %s.' % (resp_id))
    return True, None, resp_id


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

