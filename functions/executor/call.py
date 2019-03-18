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
import uuid

from include.functions_pb2 import *

def exec_function(exec_socket, status, error):
    call = FunctionCall()
    call.ParseFromString(exec_socket.recv())

    obj_id = str(uuid.uuid4())
    if not call.HasField('resp_id'):
        call.resp_id = obj_id
    else:
        obj_id = call.resp_id

    reqid = call.request_id
    fargs = _process_args(call.args)

    f = _retrieve_function(name, client)
    if not f:
        error.error = FUNC_NOT_FOUND
        exec_socket.send(error.SerializeToString())
        return

    exec_socket.send_string(obj_id)
    result = _exec_func(client, f, fargs)

    client.put(obj_id, serialize_val(result).SerializeToString())


def exec_dag_function(ctx, client, trigger, function, schedule):
    schedule = queue[trigger.name][trigger.id]
    fargs = list(schedule.arguments[trigger.name].args) + list(trigger.args)
    fargs = _process_args(fargs)

    result = _exec_func(client, function, fargs)

    result_triggers = []

    # TODO: use socket cache
    for conn in schedule.dag.connections:
        if conn.source == trigger.name:
            new_trigger = DagTrigger()
            new_trigger.id = trigger.id
            new_trigger.target_function = conn.sink

            if type(result) == tuple:
                for r in result:
                    val = Value()
                    val.type = DEFAULT
                    val.body = get_serializer(val.type).dump(r)
                    new_trigger.add_args(val)

            dest_ip = schedule.locations[conn.sink]
            sckt = ctx.socket(zmq.PUSH)
            sckt.connect(_get_dag_exec_ip(dest_ip))
            sckt.send(new_trigger.SerializeToString())


def _process_args(arg_list):
    return list(map(lambda arg: get_serializer(arg.type).load(arg.body),
        arg_list))


def _exec_func(client, func, args):
    global global_util
    start = time.time()

    func_args = ()

    # resolve any references to KVS objects
    for arg in args:
        if isinstance(arg, FluentReference):
            func_args += (_resolve_ref(arg, client),)
        else:
            func_args += (arg,)

    # execute the function
    return  func(*func_args)

def _resolve_ref(ref, client):
    ref_data = client.get(ref.key)

    # when chaining function executions, we must wait
    while not ref_data:
        ref_data = client.get(ref.key)

    refval = Value()
    refval.ParseFromString(ref_data)

    if ref.deserialize:
        refval = get_serializer(refval.type).load(refval.body)

    return refval

