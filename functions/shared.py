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

from functions_pb2 import *
from serializer import *

# shared constants
FUNCOBJ = 'funcs/index-allfuncs'
FUNC_PREFIX = 'funcs/'
BIND_ADDR_TEMPLATE = 'tcp://*:%d'

CONNECT_PORT = 4999
CREATE_PORT = 5000
CALL_PORT = 5001
LIST_PORT = 5002

class FluentFuture():
    def __init__(self, obj_id, kvs_client):
        self.obj_id = obj_id
        self.kvs_client = kvs_client

    def get(self):
        obj = self.kvs_client.get(self.obj_id)

        while not obj:
            obj = self.kvs_client.get(self.obj_id)

        retval = Value()
        retval.ParseFromString(obj)

        return get_serializer(retval.type).load(retval.body)

class FluentFunction():
    def __init__(self, name, conn, kvs_client):
        self.name = name
        self._conn = conn
        self._kvs_client = kvs_client

    def __call__(self, *args):
        obj_id = self._conn.exec_func(self.name, args)
        return FluentFuture(obj_id, self._kvs_client)

class FluentReference():
    def __init__(self, key, deserialize):
        self.key = key
        self.deserialize = deserialize

def serialize_val(val, valobj=None):
    if not valobj:
        valobj = Value()

    if isinstance(val, FluentFuture):
        valobj.body = default_ser.dump(FluentReference(val.obj_id, True))
    elif isinstance(val, np.ndarray):
        valobj.body = numpy_ser.dump(val)
        valobj.type = NUMPY
    else:
        valobj.body = default_ser.dump(val)

    return valobj
