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

from anna.lattices import *
from include.functions_pb2 import *
from include.shared import *
from include.serializer import *
from include.server_utils import *
from . import utils

def exec_function(exec_socket, kvs, status, error):
    call = FunctionCall()
    call.ParseFromString(exec_socket.recv())
    logging.info('Received call for ' + call.name)

    obj_id = str(uuid.uuid4())
    if not call.HasField('resp_id'):
        call.resp_id = obj_id
    else:
        obj_id = call.resp_id

    reqid = call.request_id
    fargs = _process_args(call.args)

    f = utils._retrieve_function(call.name, kvs)
    if not f:
        error.error = FUNC_NOT_FOUND
        exec_socket.send(error.SerializeToString())
        return

    resp = GenericResponse()
    resp.success = True
    resp.response_id = obj_id

    exec_socket.send(resp.SerializeToString())
    result = _exec_func(kvs, f, fargs)
    result = serialize_val(result)

    result_lattice = LWWPairLattice(generate_timestamp(0), result)
    kvs.put(obj_id, result_lattice)


def exec_dag_function(ctx, kvs, trigger, function, schedule):
    schedule = queue[trigger.name][trigger.id]
    fargs = list(schedule.arguments[trigger.name].args) + list(trigger.args)
    fargs = _process_args(fargs)

    result = _exec_func(kvs, function, fargs)

    result_triggers = []

    # TODO: use socket cache
    is_sink = True
    for conn in schedule.dag.connections:
        if conn.source == trigger.name:
            is_sink = False
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
            sckt.connect(_get_dag_trigger_address(dest_ip))
            sckt.send(new_trigger.SerializeToString())

    if is_sink:
        l = LWWPairLattice(generate_timestamp(0), result)
        kvs.put(schedule.response_id, l)


def _process_args(arg_list):
    return list(map(lambda arg: get_serializer(arg.type).load(arg.body),
        arg_list))


def _exec_func(kvs, func, args):
    func_args = ()

    # resolve any references to KVS objects
    for arg in args:
        if isinstance(arg, FluentReference):
            func_args += (_resolve_ref(arg, kvs),)
        else:
            func_args += (arg,)

    # execute the function
    return  func(*func_args)

def _resolve_ref(ref, kvs):
    ref_data = kvs.get(ref.key)

    # when chaining function executions, we must wait
    while not ref_data:
        ref_data = kvs.get(ref.key)

    refval = Value()
    refval.ParseFromString(ref_data)

    if ref.deserialize:
        refval = get_serializer(refval.type).load(refval.body)

    return refval

