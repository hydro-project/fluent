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
    msg_socket = ctx.socket(zmq.REP)
    msg_socket.bind(BIND_ADDR_TEMPLATE % (MSG_PORT))

    routing_addr = os.environ['ROUTE_ADDR']
    mgmt_ip = os.environ['MGMT_IP']
    ip = os.environ['MY_IP']

    client = AnnaClient(routing_addr, ip)

    report_start = time.time()

    while True:
        msg = msg_socket.recv_string()
        logging.info('Received message: %s.' % (msg))
        args = msg.split('|')

        if args[0] == 'create':
            name = _get_func_kvs_name(args[1])
            body = deserialize(args[2])

            logging.info('Creating function %s.' % (name))
            client.put(name, body)

            funcs = _get_func_list(client, '', fullname=True)
            funcs.append(name)
            _put_func_list(client, funcs)

            msg_socket.send_string('Success!')

        if args[0] == 'call':
            name = _get_func_kvs_name(args[1])
            logging.info('Executing function %s.' % (name))

            if len(args) > 3:
                reqid = args[2]
                fargs = load(args[3])
            else:
                fargs = load(args[2])


            obj_id = str(uuid.uuid4())
            msg_socket.send_string(obj_id)

            _exec_func(name, obj_id, fargs, reqid)

        if args[0] == 'list':
            prefix = args[1] if len(args) > 1 else ''

            logging.info('Retrieving functions with prefix %s.' % (prefix))
            msg_socket.send_string(dump(_get_func_list(client, prefix)))

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

    result = []
    prefix = FUNC_PREFIX + prefix

    for f in funcs:
        if f.startswith(prefix):
            if fullname:
                result.append(f)
            else:
                result.append(f[6:])

    return result

def _put_func_list(client, funclist):
    client.put(FUNCOBJ, cp.dumps(list(set(funclist))))

def _get_func_kvs_name(fname):
    return FUNC_PREFIX + fname

def _exec_func(funcname, obj_id, arg_obj, reqid):
    global global_util
    start = time.time()
    func_binary = client.get(funcname)

    func = cp.loads(func_binary)
    args = cp.loads(arg_obj)

    func_args = ()
    logging.info('Executing function %s (%s).\n' % (funcname, reqid))

    for arg in args:
        if isinstance(arg, SkyReference):
            func_args += (_resolve_ref(arg, flog, client),)
        else:
            func_args += (arg,)

    res = func(*func_args)
    logging.info('Putting result %s into KVS at id %s (%s).\n' % (str(res),
        obj_id, reqid))

    client.put(obj_id, cp.dumps(res))
    end = time.time()
    global_util += (end - start)

def _resolve_ref(ref, flog, client):
    ref_data = client.get(ref.key)

    # when chaining function executions, we must wait
    while not ref_data:
        ref_data = client.get(ref.key)

    logging.info('Resolved reference %s with value is %s.' % (key.ref, cp.loads(ref_data)))

    if ref.deserialize:
        return cp.loads(ref_data)
    else:
        return ref_data

if __name__ == '__main__':
    run()
