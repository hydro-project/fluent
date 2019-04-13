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
import time
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

    fargs = _process_args(call.args)

    f = utils._retrieve_function(call.name, kvs)
    if not f:
        logging.info('Function %s not found! Putting an error.' %
                (call.name))
        sutils.error.error = FUNC_NOT_FOUND
        result = serialize_val(('ERROR', sutils.error.SerializeToString()))
    else:
        try:
            result = _exec_func_normal(kvs, f, fargs)
            result = serialize_val(result)
        except Exception as e:
            logging.info('Unexpected error %s while executing function.' %
                    (str(e)))
            sutils.error.error = EXEC_ERROR
            result = serialize_val(('ERROR: ' + str(e),
                    sutils.error.SerializeToString()))

    result_lattice = LWWPairLattice(generate_timestamp(0), result)
    kvs.put(call.resp_id, result_lattice)


def exec_dag_function(pusher_cache, kvs, triggers, function, schedule):
    if (schedule.consistency == NORMAL):
        _exec_dag_function_normal(pusher_cache, kvs,
                                  triggers, function, schedule)
    else:
        _exec_dag_function_causal(pusher_cache, kvs,
                                  triggers, function, schedule)

def _exec_dag_function_normal(pusher_cache, kvs, triggers, function, schedule):
    fname = schedule.target_function
    fargs = list(schedule.arguments[fname].args)

    for trname in schedule.triggers:
        trigger = triggers[trname]
        fargs += list(trigger.arguments.args)

    logging.info('Executing function %s for DAG %s (ID %s): started at %.6f.' %
            (schedule.dag.name, fname, trigger.id, time.time()))

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

    logging.info('Finished executing function %s for DAG %s (ID %s): started at %.6f.' %
            (schedule.dag.name, fname, trigger.id, time.time()))
    if is_sink:
        logging.info('DAG %s (ID %s) completed; result at %s.' %
                (schedule.dag.name, trigger.id, schedule.id))
        l = LWWPairLattice(generate_timestamp(0), serialize_val(result))
        kvs.put(schedule.id, l)

def _exec_func_normal(kvs, func, args):
    refs = list(filter(lambda a: isinstance(a, FluentReference), args))
    if refs:
        refs = _resolve_ref_normal(refs, kvs)

    func_args = ()
    for arg in args:
        if isinstance(arg, FluentReference):
            func_args += (refs[arg.key],)
        else:
            func_args += (arg,)

    # execute the function
    return  func(*func_args)

def _resolve_ref_normal(refs, kvs):
    keys = [ref.key for ref in refs]
    kv_pairs = kvs.get(keys)

    # when chaining function executions, we must wait
    while not kv_pairs:
        kv_pairs = kvs.get(keys)

    for ref in refs:
        if ref.deserialize and isinstance(kv_pairs[ref.key], LWWPairLattice):
            kv_pairs[ref.key] = deserialize_val(kv_pairs[ref.key].reveal()[1])

    return kv_pairs

def _exec_dag_function_causal(pusher_cache, kvs, triggers, function, schedule):
    fname = schedule.target_function
    fargs = list(schedule.arguments[fname].args)

    versioned_key_locations = None
    dependencies = {}

    for trname in schedule.triggers:
        trigger = triggers[trname]
        fargs += list(trigger.arguments.args)
        # combine versioned_key_locations
        if versioned_key_locations is None:
            versioned_key_locations = trigger.versioned_key_locations
        else:
            for addr in trigger.versioned_key_locations:
                versioned_key_locations[addr].versioned_keys.extend(
                        trigger.versioned_key_locations[addr].versioned_keys)
        # combine dependencies from previous func
        for dep in trigger.dependencies:
            if dep.key in dependencies:
                dependencies[dep.key] = \
                _merge_vector_clock(dependencies[dep.key], dep.vector_clock)
            else:
                dependencies[dep.key] = dep.vector_clock

    logging.info('Executing function %s for DAG %s (ID %d) in ' +
        'causal consistency.' % (schedule.dag.name, fname, trigger.id))

    fargs = _process_args(fargs)

    kv_pairs = {}

    result = _exec_func_causal(kvs, function, fargs, kv_pairs,
                               schedule, versioned_key_locations)

    for key in kv_pairs:
        if key in dependencies:
            dependencies[key] = _merge_vector_clock(dependencies[key],
                                                    kv_pairs[key][0])
        else:
            dependencies[key] = kv_pairs[key][0]

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

            new_trigger.versioned_key_locations = versioned_key_locations

            for key in dependencies:
                dep = new_trigger.dependencies.add()
                dep.key = key
                dep.vector_clock = dependencies[key]

            dest_ip = schedule.locations[conn.sink]
            sckt = pusher_cache.get(sutils._get_dag_trigger_address(dest_ip))
            sckt.send(new_trigger.SerializeToString())

    if is_sink:
        logging.info('DAG %s (ID %d) completed in causal mode; result at %s.' %
                (schedule.dag.name, trigger.id, schedule.response_id))

        vector_clock = {}
        if schedule.response_id in dependencies:
            if schedule.id in dependencies[schedule.response_id]:
                dependencies[schedule.response_id][schedule.id] += 1
            else:
                dependencies[schedule.response_id][schedule.id] = 1
            vector_clock = dependencies[schedule.response_id][schedule.id]
            del dependencies[schedule.response_id]
        else:
            vector_clock = {schedule.id : 1}

        succeed = kvs.causal_put(schedule.response_id,
                                 vector_clock, dependencies,
                                 serialize_val(result), schedule.id)
        while not succeed:
            kvs.causal_put(schedule.response_id, vector_clock,
                           dependencies, serialize_val(result), schedule.id)



def _exec_func_causal(kvs, func, args, kv_pairs,
                      schedule, versioned_key_locations):
    func_args = ()
    to_resolve = []
    deserialize = {}

    # resolve any references to KVS objects
    key_index_map = {}
    for i, arg in enumerate(args):
        if isinstance(arg, FluentReference):
            to_resolve.append(arg)
            key_index_map[arg.key] = i
            deserialize[arg.key] = arg.deserialize
        func_args += (arg,)

    if len(to_resolve) > 0:
        _resolve_ref_causal(to_resolve, kvs, kv_pairs,
                            schedule, versioned_key_locations)

        for key in kv_pairs:
            if deserialize[key]:
                func_args[key_index_map[key]] = \
                                deserialize_val(kv_pairs[key][1])
            else:
                func_args[key_index_map[key]] = kv_pairs[key][1]

    # execute the function
    return  func(*func_args)

def _resolve_ref_causal(refs, kvs, kv_pairs, schedule, versioned_key_locations):
    future_read_set = _compute_children_read_set(schedule)
    keys = [ref.key for ref in refs]
    result = kvs.causal_get(keys, future_read_set,
                            versioned_key_locations,
                            schedule.consistency, schedule.id)

    while not result:
        result = kvs.causal_get(refs, future_read_set,
                                versioned_key_locations,
                                schedule.consistency, schedule.id)

    if result[0] is not None:
        versioned_key_locations[result[0][0]] = list(result[0][1])

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
