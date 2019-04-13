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

import time

from .functions_pb2 import *
from . import serializer

CONNECT_PORT = 5000
FUNC_CREATE_PORT = 5001
FUNC_CALL_PORT = 5002
LIST_PORT = 5003
DAG_CREATE_PORT = 5004
DAG_CALL_PORT = 5005

def generate_timestamp(tid=1):
    t = time.time()

    p = 10
    while tid >= p:
        p *= 10

    return int(t * p + tid)

class FluentFuture():
    def __init__(self, obj_id, kvs_client):
        self.obj_id = obj_id
        self.kvs_client = kvs_client

    def get(self):
        obj = self.kvs_client.get(self.obj_id)

        while not obj:
            obj = self.kvs_client.get(self.obj_id)

        return serializer.deserialize_val(obj.reveal()[1])

class FluentFunction():
    def __init__(self, name, conn, kvs_client):
        self.name = name
        self._conn = conn
        self._kvs_client = kvs_client

    def __call__(self, *args):
        obj_id = self._conn.exec_func(self.name, args)
        return FluentFuture(obj_id, self._kvs_client)

class FluentReference():
    def __init__(self, key, deserialize, obj_type):
        self.key = key
        self.deserialize = deserialize
        self.obj_type = obj_type
