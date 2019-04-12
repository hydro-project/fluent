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

import boto3
import cloudpickle as cp
import numpy
import zmq

from anna.client import AnnaClient
from include.functions_pb2 import *
from include.shared import *
from include.serializer import *

class FluentConnection():
    def __init__(self, func_addr, ip=None, tid=0):
        self.service_addr = 'tcp://'+  func_addr + ':%d'
        self.context = zmq.Context(1)
        kvs_addr = self._connect()

        if ip:
            self.kvs_client = AnnaClient(kvs_addr, ip, offset=tid)
        else:
            self.kvs_client = AnnaClient(kvs_addr, offset=tid)

        self.func_create_sock = self.context.socket(zmq.REQ)
        self.func_create_sock.connect(self.service_addr % FUNC_CREATE_PORT)

        self.func_call_sock = self.context.socket(zmq.REQ)
        self.func_call_sock.connect(self.service_addr % FUNC_CALL_PORT)

        self.list_sock = self.context.socket(zmq.REQ)
        self.list_sock.connect(self.service_addr % LIST_PORT)

        self.dag_create_sock = self.context.socket(zmq.REQ)
        self.dag_create_sock.connect(self.service_addr % DAG_CREATE_PORT)

        self.dag_call_sock = self.context.socket(zmq.REQ)
        self.dag_call_sock.connect(self.service_addr % DAG_CALL_PORT)

        self.rid = 0

    def _connect(self):
        sckt = self.context.socket(zmq.REQ)
        sckt.connect(self.service_addr % CONNECT_PORT)
        sckt.send_string('')

        return sckt.recv_string()

    def list(self, prefix=None):
        for fname in self._get_func_list(prefix):
            print(fname)

    def get(self, name):
        if name not in self._get_func_list():
            print("No function found with name '" + name + "'.")
            print("To view all functions, use the `list` method.")
            return None

        return FluentFunction(name, self, self.kvs_client)

    def _get_func_list(self, prefix=None):
        msg = prefix if prefix else ''
        self.list_sock.send_string(msg)

        flist = FunctionList()
        flist.ParseFromString(self.list_sock.recv())
        return flist

    def exec_func(self, name, args):
        call = FunctionCall()
        call.name = name
        call.request_id = self.rid

        for arg in args:
            argobj = call.args.add()
            serialize_val(arg, argobj)

        self.func_call_sock.send(call.SerializeToString())

        r = GenericResponse()
        r.ParseFromString(self.func_call_sock.recv())

        self.rid += 1
        return r.response_id

    def register(self, function, name):
        func = Function()
        func.name = name
        func.body = function_ser.dump(function)

        self.func_create_sock.send(func.SerializeToString())

        resp = GenericResponse()
        resp.ParseFromString(self.func_create_sock.recv())

        if resp.success:
            return FluentFunction(name, self, self.kvs_client)
        else:
            print('Unexpected error while registering function: \n\t%s.'
                    % (resp))

    def register_dag(self, name, functions, connections):
        flist = self._get_func_list()
        for fname in functions:
            if fname not in flist.names:
                print(('Function %s not registered. Please register before'
                        + 'including it in a DAG.') % (fname))
                return False, None

        dag = Dag()
        dag.name = name
        dag.functions.extend(functions)
        for pair in connections:
            conn = dag.connections.add()
            conn.source = pair[0]
            conn.sink = pair[1]

        self.dag_create_sock.send(dag.SerializeToString())

        r = GenericResponse()
        r.ParseFromString(self.dag_create_sock.recv())

        return r.success, r.error

    def call_dag(self, dname, arg_map):
        dc = DagCall()
        dc.name = dname

        for fname in arg_map:
            args = [serialize_val(arg, serialize=False) for arg in
                    arg_map[fname]]
            al = dc.function_args[fname]
            al.args.extend(args)

        self.dag_call_sock.send(dc.SerializeToString())

        r = GenericResponse()
        r.ParseFromString(self.dag_call_sock.recv())

        if r.success:
            return r.response_id
        else:
            return None