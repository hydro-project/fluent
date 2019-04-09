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
import zmq

from anna.lattices import *
from include.functions_pb2 import *
from include.shared import *
from include.serializer import *
from include import server_utils as sutils
from . import utils

def _process_args(arg_list):
    return list(map(lambda v: get_serializer(v.type).load(v.body), arg_list))

def exec_function(exec_socket, kvs, status):
    call = FunctionCall()
    call.ParseFromString(exec_socket.recv())
    logging.info('Received call for ' + call.name)

    if not status.running:
        sutils.error.error = INVALID_TARGET
        exec_socket.send(sutils.SerializeToString())
        return

    obj_id = str(uuid.uuid4())
    if not call.HasField('resp_id'):
        call.resp_id = obj_id
    else:
        obj_id = call.resp_id

    reqid = call.request_id
    fargs = _process_args(call.args)

    f = utils._retrieve_function(call.name, kvs)
    if not f:
        sutils.error.error = FUNC_NOT_FOUND
        exec_socket.send(sutils.error.SerializeToString())
        return

    resp = GenericResponse()
    resp.success = True
    resp.response_id = obj_id

    exec_socket.send(resp.SerializeToString())
    result = _exec_func(kvs, f, fargs)
    result = serialize_val(result)

    result_lattice = LWWPairLattice(generate_timestamp(0), result)
    kvs.put(obj_id, result_lattice)


def exec_dag_function(pusher_cache, kvs, triggers, function, schedule):
    if (schedule.consistency == NORMAL):
        _exec_dag_function_normal(pusher_cache, kvs, triggers, function, schedule)
    else:
        _exec_dag_function_causal(pusher_cache, kvs, triggers, function, schedule)

def _exec_dag_function_normal(pusher_cache, kvs, triggers, function, schedule):
    fname = schedule.target_function
    fargs = list(schedule.arguments[fname].args)

    for trname in schedule.triggers:
        trigger = triggers[trname]
        fargs += list(trigger.arguments.args)

    logging.info('Executing function %s for DAG %s (ID %d).' %
            (schedule.dag.name, fname, trigger.id))

    fargs = _process_args(fargs)

    result = _exec_func_normal(kvs, function, fargs)

    is_sink = True
    for conn in schedule.dag.connections:
        if conn.source == fname:
            is_sink = False
            new_trigger = DagTrigger()
            new_trigger.id = trigger.id
            new_trigger.target_function = conn.sink
            new_trigger.source = fname

            if type(result) != tuple:
                result = (result,)

            al = new_trigger.arguments
            al.args.extend(list(map(lambda v: serialize_val(v, None, False),
                result)))

            dest_ip = schedule.locations[conn.sink]
            sckt = pusher_cache.get(sutils._get_dag_trigger_address(dest_ip))
            sckt.send(new_trigger.SerializeToString())

    if is_sink:
        logging.info('DAG %s (ID %d) completed; result at %s.' %
                (schedule.dag.name, trigger.id, schedule.response_id))
        l = LWWPairLattice(generate_timestamp(0), serialize_val(result))
        kvs.put(schedule.response_id, l)

def _exec_func_normal(kvs, func, args):
    func_args = ()

    to_resolve = []

    # resolve any references to KVS objects
    key_index_map = {}
    for i, arg in enumerate(args):
        if isinstance(arg, FluentReference):
            to_resolve.append(arg)
            key_index_map[arg.key] = i
        func_args += (arg,)

    if len(to_resolve) > 0: 
        kv_pairs = _resolve_ref_normal(to_resolve, kvs)

        for key in kv_pairs:
            func_args[key_index_map[key]] = kv_pairs[key]

    # execute the function
    return  func(*func_args)

def _resolve_ref_normal(refs, kvs):
    kv_pairs = kvs.get(refs)

    # when chaining function executions, we must wait
    while not kv_pairs:
        kv_pairs = kvs.get(refs)

    return kv_pairs

def _exec_dag_function_causal(pusher_cache, kvs, triggers, function, schedule):
    fname = schedule.target_function
    fargs = list(schedule.arguments[fname].args)

    address_to_versioned_key_list_map = {}
    dependency = {}

    for trname in schedule.triggers:
        trigger = triggers[trname]
        fargs += list(trigger.arguments.args)
        # combine address_versioned_key_list pairs
        for addr in trigger.address_to_versioned_key_list_map:
            address_to_versioned_key_list_map[addr] = list(trigger.address_to_versioned_key_list_map[addr].versioned_keys)
        # combine dependency from previous func
        for dep in trigger.dependency:
            if dep.key in dependency:
                dependency[dep.key] = _merge_vector_clock(dependency[dep.key], dep.vector_clock)
            else:
                dependency[dep.key] = dep.vector_clock

    logging.info('Executing function %s for DAG %s (ID %d) in causal consistency.' %
            (schedule.dag.name, fname, trigger.id))

    fargs = _process_args(fargs)

    kv_pairs = {}

    result = _exec_func_causal(kvs, function, fargs, kv_pairs, schedule, address_to_versioned_key_list_map)

    for key in kv_pairs:
        if key in dependency:
            dependency[key] = _merge_vector_clock(dependency[key], kv_pairs[key][0])
        else:
            dependency[key] = kv_pairs[key][0]

    is_sink = True
    for conn in schedule.dag.connections:
        if conn.source == fname:
            is_sink = False
            new_trigger = DagTrigger()
            new_trigger.id = trigger.id
            new_trigger.target_function = conn.sink
            new_trigger.source = fname

            if type(result) != tuple:
                result = (result,)

            al = new_trigger.arguments
            al.args.extend(list(map(lambda v: serialize_val(v, None, False),
                result)))

            new_trigger.address_to_versioned_key_list_map = address_to_versioned_key_list_map

            for key in dependency:
                dep = new_trigger.dependency.add()
                dep.key = key
                dep.vector_clock = dependency[key]

            dest_ip = schedule.locations[conn.sink]
            sckt = pusher_cache.get(sutils._get_dag_trigger_address(dest_ip))
            sckt.send(new_trigger.SerializeToString())

    if is_sink:
        logging.info('DAG %s (ID %d) completed in causal mode; result at %s.' %
                (schedule.dag.name, trigger.id, schedule.response_id))

        vector_clock = {}
        if schedule.response_id in dependency:
            if schedule.id in dependency[schedule.response_id]:
                dependency[schedule.response_id][schedule.id] += 1
            else:
                dependency[schedule.response_id][schedule.id] = 1
            vector_clock = dependency[schedule.response_id][schedule.id]
            del dependency[schedule.response_id]
        else:
            vector_clock = {schedule.id : 1}

        succeed = kvs.causal_put(schedule.response_id, vector_clock, dependency, serialize_val(result), schedule.id)
        while not succeed:
            kvs.causal_put(schedule.response_id, vector_clock, dependency, serialize_val(result), schedule.id)



def _exec_func_causal(kvs, func, args, kv_pairs, schedule, address_to_versioned_key_list_map):
    func_args = ()

    to_resolve = []

    # resolve any references to KVS objects
    key_index_map = {}
    for i, arg in enumerate(args):
        if isinstance(arg, FluentReference):
            to_resolve.append(arg)
            key_index_map[arg.key] = i
        func_args += (arg,)

    if len(to_resolve) > 0: 
        _resolve_ref_causal(to_resolve, kvs, kv_pairs, schedule, address_to_versioned_key_list_map)

        for key in kv_pairs:
            func_args[key_index_map[key]] = kv_pairs[key][1]

    # execute the function
    return  func(*func_args)


def _resolve_ref_causal(refs, kvs, kv_pairs, schedule, address_to_versioned_key_list_map):
    future_read_set = _compute_children_read_set(schedule)
    result = kvs.causal_get(refs, future_read_set, schedule.consistency, schedule.id)

    while not result:
        result = kvs.causal_get(refs, future_read_set, address_to_versioned_key_list_map, schedule.consistency, schedule.id)

    if result[0] is not None:
        address_to_versioned_key_list_map[result[0][0]] = list(result[0][1])

    kv_pairs = result[1]

def _compute_children_read_set(schedule):
    future_read_set = set()
    fname = schedule.target_function
    children = set()
    delta = set(fname)

    while not len(delta) == 0:
        new_delta = set()
        for conn in schedule.dag.connections:
            if conn.source in delta and not conn.sink in children:
                children.add(conn.sink)
                new_delta.add(conn.sink)
        delta = new_delta

    for child in children:
        fargs = list(schedule.arguments[child].args)
        refs = list(filter(lambda arg: type(arg) == FluentReference,
            map(lambda arg: get_serializer(arg.type).load(arg.body),
                fargs)))
        (future_read_set.add(ref.key) for ref in refs)

    return future_read_set

def _merge_vector_clock(lhs, rhs):
    result = lhs
    for cid in rhs:
        if cid not in result:
            result[cid] = rhs[cid]
        else:
            result[cid] = max(result[cid], rhs[cid])
    return result