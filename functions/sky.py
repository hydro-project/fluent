from anna.client import AnnaClient
import cloudpickle as cp
import boto3
import requests

class SkyConnection():
    def __init__(self, func_addr, kvs_addr, port=6000):
        self.service_addr = 'http://'+  func_addr + ":" + str(port)
        self.kvs_client = AnnaClient(kvs_addr)

        self.session = requests.Session()

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
        if prefix == None:
            addr = self.service_addr + '/list'
            r = self.session.get(addr)
        else:
            r = self.session.get(self.service_addr + "/list/" + prefix)
        return cp.loads(r.content)


    def _name_to_handle(self, name):
        return name

    def exec_func(self, handle, args):
        args_bin = cp.dumps(args)

        r = self.session.post(self.service_addr + "/" + handle, data=args_bin)
        return cp.loads(r.content)

    def register(self, func, name):
        addr = self.service_addr + '/create/' + name
        self.session.post(addr,
                data=cp.dumps(func))

    def deregister(self, name):
        self.session.post(self.service_addr + "/remove/" + name)


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
