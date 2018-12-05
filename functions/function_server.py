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

from anna.client import AnnaClient
from client import SkyReference
import cloudpickle as cp
from functions_pb2 import *
import logging
import os
from shared import *
from threading import Thread
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
            args = msg.split('|')
            prefix = args[1] if len(args) > 1 else ''

            logging.info('Retrieving functions with prefix %s.' % (prefix))
            resp = FunctionList()
            resp.names.append(_get_func_list(client, prefix))

            list_socket.send(resp.SerializeToString())

        # periodically report function occupancy
        report_end = time.time()
        if report_end - report_start > REPORT_THRESH:
            util = global_util / REPORT_THRESH

            sckt = ctx.socket(zmq.PUSH)
            sckt.connect('tcp://' + mgmt_ip + ':7002')
            sckt.send_string(str(util))

            logging.info('Sending utilization of %.2f%%.' % (util))

            report_start = time.time()
            global_util = 0

def _get_func_list(client, prefix, fullname=False):
    funcs = client.get(FUNCOBJ)
    if len(funcs) == 0:
        return []
    funcs = cp.loads(funcs)

    prefix = FUNC_PREFIX + prefix
    result = list(filter(lambda fn: fn.startswith(prefix), funcs))

    if not fullname:
        result = list(map(lambda fn: fn.split(FUNC_PREFIX)[-1], result))

    return result

def _put_func_list(client, funclist):
    client.put(FUNCOBJ, cp.dumps(list(set(funclist))))

def _get_func_kvs_name(fname):
    return FUNC_PREFIX + fname

def _exec_func(client, funcname, obj_id, args, reqid):
    global global_util
    start = time.time()

    func = default_ser.load(client.get(funcname))

    func_args = ()
    logging.info('Executing function %s (%s).' % (funcname, reqid))

    for arg in args:
        if isinstance(arg, SkyReference):
            func_args += (_resolve_ref(arg, client),)
        else:
            func_args += (arg,)

    res = func(*func_args)
    logging.info('Putting result %s into KVS at id %s (%s).' % (str(res),
        obj_id, reqid))

    client.put(obj_id, cp.dumps(res))
    end = time.time()
    global_util += (end - start)

def _resolve_ref(ref, client):
    ref_data = client.get(ref.key)

    # when chaining function executions, we must wait
    while not ref_data:
        ref_data = client.get(ref.key)

    logging.info('Resolved reference %s with value is %s.' % (ref.key, cp.loads(ref_data)))

    if ref.deserialize:
        return cp.loads(ref_data)
    else:
        return ref_data

if __name__ == '__main__':
    run()
