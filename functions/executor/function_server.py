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

from anna.local_client import AnnaClient
from cache import LruCache
from ..shared.functions_pb2 import *
import logging
import os
from shared import *
import time
import uuid
import zmq

REPORT_THRESH = 30
global_util = 0.0

logging.basicConfig(filename='log.txt', level=logging.INFO)

def run():
    global global_util

    ctx = zmq.Context(1)
    connect_socket = ctx.socket(zmq.REP)
    connect_socket.bind(BIND_ADDR_TEMPLATE % (CONNECT_PORT))

    create_socket = ctx.socket(zmq.REP)
    create_socket.bind(BIND_ADDR_TEMPLATE % (CREATE_PORT))

    call_socket = ctx.socket(zmq.REP)
    call_socket.bind(BIND_ADDR_TEMPLATE % (CALL_PORT))

    list_socket = ctx.socket(zmq.REP)
    list_socket.bind(BIND_ADDR_TEMPLATE % (LIST_PORT))

    poller = zmq.Poller()
    poller.register(connect_socket, zmq.POLLIN)
    poller.register(create_socket, zmq.POLLIN)
    poller.register(call_socket, zmq.POLLIN)
    poller.register(list_socket, zmq.POLLIN)

    routing_addr = os.environ['ROUTE_ADDR']
    mgmt_ip = os.environ['MGMT_IP']
    ip = os.environ['MY_IP']

    client = AnnaClient(routing_addr, ip)

    report_start = time.time()

    while True:
        socks = dict(poller.poll(timeout=1000))

        if connect_socket in socks and socks[connect_socket] == zmq.POLLIN:
            logging.info('Received connection request.')
            msg = connect_socket.recv_string()
            connect_socket.send_string(routing_addr)

        if create_socket in socks and socks[create_socket] == zmq.POLLIN:
            func = Function()
            func.ParseFromString(create_socket.recv())

            name = _get_func_kvs_name(func.name)
            logging.info('Creating function %s.' % (name))
            client.put(name, func.body)

            funcs = _get_func_list(client, '', fullname=True)
            funcs.append(name)
            _put_func_list(client, funcs)

            create_socket.send_string('Success!')

        if call_socket in socks and socks[call_socket] == zmq.POLLIN:
            call = FunctionCall()
            call.ParseFromString(call_socket.recv())

            name = _get_func_kvs_name(call.name)
            reqid = call.request_id;
            fargs = list(map(lambda arg:
                get_serializer(arg.type).load(arg.body), call.args))

            obj_id = str(uuid.uuid4())
            call_socket.send_string(obj_id)

            _exec_func(client, name, obj_id, fargs, reqid)

        if list_socket in socks and socks[list_socket] == zmq.POLLIN:
            msg = list_socket.recv_string()
            prefix = msg if msg else ''

            resp = FunctionList()
            resp.names.append(_get_func_list(client, prefix))

            list_socket.send(resp.SerializeToString())

        # periodically report function occupancy
        report_end = time.time()
        if report_end - report_start > REPORT_THRESH:
            util = global_util / (report_end - report_start)

            sckt = ctx.socket(zmq.PUSH)
            sckt.connect('tcp://' + mgmt_ip + ':7002')
            sckt.send_string(ip + '|' + str(util))

            logging.info('Sending utilization of %.2f%%.' % (util * 100))

            report_start = time.time()
            global_util = 0

def _get_func_list(client, prefix, fullname=False):
    funcs = client.get(FUNCOBJ)
    if len(funcs) == 0:
        return []
    funcs = default_ser.load(funcs)

    prefix = FUNC_PREFIX + prefix
    result = list(filter(lambda fn: fn.startswith(prefix), funcs))

    if not fullname:
        result = list(map(lambda fn: fn.split(FUNC_PREFIX)[-1], result))

    return result

def _put_func_list(client, funclist):
    client.put(FUNCOBJ, default_ser.dump(list(set(funclist))))

def _get_func_kvs_name(fname):
    return FUNC_PREFIX + fname

def _exec_func(client, funcname, obj_id, args, reqid):
    global global_util, function_cache, data_cache
    start = time.time()
    logging.info('Executing function %s (%s).' % (funcname, reqid))

    # load the function from the KVS if it is not cached locally
    func = function_cache.get(funcname)
    if not func:
        func = function_ser.load(client.get(funcname))
        function_cache.put(funcname, func)

    func_args = ()

    # resolve any references to KVS objects
    for arg in args:
        if isinstance(arg, FluentReference):
            func_args += (_resolve_ref(arg, client),)
        else:
            func_args += (arg,)

    # execute the function
    res = func(*func_args)
    data_cache.put(obj_id, res)

    # reserialize the result and put it back into the KVS
    res = serialize_val(res).SerializeToString()
    client.put(obj_id, res)


    end = time.time()
    global_util += (end - start)

def _resolve_ref(ref, client):
    global data_cache

    refval = data_cache.get(ref.key)
    if refval is not None:
        return refval

    ref_data = client.get(ref.key)

    # when chaining function executions, we must wait
    while not ref_data:
        ref_data = client.get(ref.key)

    refval = Value()
    refval.ParseFromString(ref_data)

    if ref.deserialize:
        refval = get_serializer(refval.type).load(refval.body)

    data_cache.put(ref.key, refval)

    return refval

if __name__ == '__main__':
    run()
