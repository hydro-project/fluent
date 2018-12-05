from anna.client import AnnaClient
import boto3
import cloudpickle as cp
from functions_pb2 import *
import numpy
from shared import *
import zmq

class SkyConnection():
    def __init__(self, func_addr, ip=None):
        self.service_addr = 'tcp://'+  func_addr + ':%d'
        self.context = zmq.Context(1)
        kvs_addr = self._connect()

        if ip:
            self.kvs_client = AnnaClient(kvs_addr, ip)
        else:
            self.kvs_client = AnnaClient(kvs_addr)

        self.create_sock = self.context.socket(zmq.REQ)
        self.create_sock.connect(self.service_addr % CREATE_PORT)

        self.call_sock = self.context.socket(zmq.REQ)
        self.call_sock.connect(self.service_addr % CALL_PORT)

        self.list_sock = self.context.socket(zmq.REQ)
        self.list_sock.connect(self.service_addr % LIST_PORT)

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

        return SkyFunc(name, self, self.kvs_client)

    def _get_func_list(self, prefix=None):
        msg = 'list|' + (prefix if prefix else '')
        self.list_sock.send_string(msg)

        flist = FunctionList()
        flist.ParseFromString(self.list_sock.recv())
        return flist

    def _process_arg(self, arg):
        resp = FunctionCall.Argument()

        if isinstance(arg, SkyFuture):
            resp.body = default_ser.dump(SkyReference(arg.obj_id, True))
        elif isinstance(arg, numpy.ndarray):
            resp.body = numpy_ser.dump(arg)
            resp.type = NUMPY
        else:
            resp.body = default_ser.dump(arg)

        return resp

    def exec_func(self, name, args):
        call = FunctionCall()
        map(lambda arg: call.args.append(self._process_arg(arg)), args)
        call.name = name
        call.request_id = self.rid

        self.call_sock.send(call.SerializeToString())

        self.rid += 1
        return self.call_sock.recv_string()

    def register(self, function, name):
        func = Function()
        func.name = name
        func.body = default_ser.dump(function)

        self.create_sock.send(func.SerializeToString())

        resp = self.create_sock.recv_string()

        if 'Success' in resp:
            return SkyFunc(name, self, self.kvs_client)
        else:
            print('Unexpected error while registering function: \n\t%s.'
                    % (resp))

class SkyFuture():
    def __init__(self, obj_id, kvs_client):
        self.obj_id = obj_id
        self.kvs_client = kvs_client

    def get(self):
        obj = self.kvs_client.get(self.obj_id)

        while not obj:
            obj = self.kvs_client.get(self.obj_id)

        return cp.loads(obj)

class SkyFunc():
    def __init__(self, name, conn, kvs_client):
        self.name = name
        self._conn = conn
        self._kvs_client = kvs_client

    def __call__(self, *args):
        obj_id = self._conn.exec_func(self.name, args)
        return SkyFuture(obj_id, self._kvs_client)

class SkyReference():
    def __init__(self, key, deserialize):
        self.key = key
        self.deserialize = deserialize
