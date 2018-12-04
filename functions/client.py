from anna.client import AnnaClient
import boto3
import cloudpickle as cp
from shared import *
import zmq

class SkyConnection():
    def __init__(self, func_addr, kvs_addr, ip=None, port=6000):
        if ip:
            self.kvs_client = AnnaClient(kvs_addr, ip)
        else:
            self.kvs_client = AnnaClient(kvs_addr)

        service_addr = 'tcp://'+  func_addr + ":" + str(port)
        context = zmq.Context(1)
        self.req_socket = context.socket(zmq.REQ)
        self.req_socket.connect(service_addr)

        self.rid = 0

    def list(self, prefix=None):
        for fname in self._get_func_list(prefix):
            print(fname)

    def get(self, name):
        if name not in self._get_func_list():
            print("No function found with name '" + name + "'.")
            print("To view all functions, use the `list` method.")
            return None

        return SkyFunc(name, self._name_to_handle(name), self, self.kvs_client)

    def _get_func_list(self, prefix=None):
        msg = 'list' + (prefix if prefix else '')
        self.req_socket.send_string(msg)

        return load(self.req_socket.recv_string())

    def _name_to_handle(self, name):
        return name

    def exec_func(self, handle, args):
        args = list(map(lambda arg: SkyReference(arg.obj_id, True) if
            isinstance(arg, SkyFuture) else arg, args))

        msg = 'call|' + str(self.reqid) + '|' + handle + '|' + dump(args)
        self.req_socket.send_string(msg)

        self.req_id += 1
        return self.req_socket.recv_string()

    def register(self, func, name):
        msg = 'create|' + name + '|' + dump(func)
        self.req_socket.send_string(msg)

        resp = self.req_socket.recv_string()

        if resp.contains('Success'):
            return SkyFunc(name, self._name_to_handle(name), self.
                    self.kvs_client)
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
    def __init__(self, name, func_handle, conn, kvs_client):
        self.name = name
        self.handle = func_handle
        self._conn = conn
        self._kvs_client = kvs_client

    def __call__(self, *args):
        obj_id = self._conn.exec_func(self.handle, args)
        return SkyFuture(obj_id, self._kvs_client)

class SkyReference():
    def __init__(self, key, deserialize):
        self.key = key
        self.deserialize = deserialize

def connect(addr=None, port=7000, pr_type='FS', pr_info={'dir': 'funcs/'}):
    return SkyConnection(addr, port, pr_type, pr_info)
