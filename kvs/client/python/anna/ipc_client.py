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

GET_REQUEST_ADDR = "ipc:///requests/get"
PUT_REQUEST_ADDR = "ipc:///requests/put"

GET_RESPONSE_ADDR_TEMPLATE = "ipc:///responses_%d/get"
PUT_RESPONSE_ADDR_TEMPLATE = "ipc:///responses_%d/put"

TIMEOUT = 5000

from .functions_pb2 import *
from .kvs_pb2 import *
from .lattices import *
import zmq

class IpcAnnaClient:
    def __init__(self, thread_id = 0, offset=0):
        self.context = zmq.Context(1)

        self.get_push_socket = self.context.socket(zmq.PUSH)
        self.get_push_socket.connect(GET_REQUEST_ADDR)

        self.put_push_socket = self.context.socket(zmq.PUSH)
        self.put_push_socket.connect(PUT_REQUEST_ADDR)

        self.get_pull_socket = self.context.socket(zmq.PULL)
        self.get_pull_socket.bind(GET_RESPONSE_ADDR_TEMPLATE % thread_id)
        self.get_pull_socket.RCVTIMEO = TIMEOUT

        self.put_pull_socket = self.context.socket(zmq.PULL)
        self.put_pull_socket.bind(PUT_RESPONSE_ADDR_TEMPLATE % thread_id)
        self.put_pull_socket.RCVTIMEO = TIMEOUT

    def get(self, refs):
        request = KeyRequest()
        request.type = GET

        deserialize = {}

        for ref in refs:
            tp = request.tuples.add()
            tp.key = ref.key
            tp.lattice_type = ref.obj_type

            deserialize[ref.key] = ref.deserialize

            if ref.obj_type == CROSSCAUSAL:
                print("Error: found cross causal lattice type in non causal mode!")

        request.response_address = GET_RESPONSE_ADDR_TEMPLATE % thread_id

        self.get_push_socket.send(request.SerializeToString())

        try:
            msg = self.get_pull_socket.recv()
        except zmq.ZMQError as e:
            if e.errno == zmq.EAGAIN:
                print("request timed out")
                return None
            else:
                print("unknown zmq error")
                return None
        else:
            kv_pairs = {}
            resp = KeyResponse()
            resp.ParseFromString(msg)

            for tp in resp.tuples:
                if tp.error == 1:
                    print('Key %s does not exist!' % (key))
                    return None

                if tp.lattice_type == LWW:
                    val = LWWValue()
                    val.ParseFromString(tp.payload)

                    if deserialize[tp.key]:
                        kv_pairs[tp.key] = deserialize_val(val.value)
                    else:
                        kv_pairs[tp.key] = val.value
                elif tp.lattice_type == SET:
                    res = set()

                    val = SetValue()
                    val.ParseFromString(tp.payload)

                    for v in val.values:
                        res.add(v)

                    # for set lattice, just pick the first one to return for now
                    # we are not using it anyway..
                    if deserialize[tp.key]:
                        kv_pairs[tp.key] = deserialize_val(res[0])
                    else:
                        kv_pairs[tp.key] = res[0]
                else:
                    raise ValueError('Invalid Lattice type: ' + str(tp.lattice_type))
            return kv_pairs

    def causal_get(self, refs, future_read_set, address_to_versioned_key_list_map, consistency, client_id):
        request = CausalRequest()

        if consistency == SINGLE_OBJ_CAUSAL:
            request.consistency = SINGLE
        elif consistency == CROSS_OBJ_CAUSAL:
            request.consistency = CROSS
        else:
            print("Error: non causal consistency in causal mode!")
            return None

        request.id = str(client_id)

        request.address_to_versioned_key_list_map = address_to_versioned_key_list_map

        deserialize = {}

        for ref in refs:
            tp = request.tuples.add()
            tp.key = ref.key

            deserialize[ref.key] = ref.deserialize

            if not (ref.obj_type == CAUSAL or ref.obj_type == CROSSCAUSAL):
                print("Error: found other lattice type in causal mode!")
                return None

        request.response_address = GET_RESPONSE_ADDR_TEMPLATE % thread_id

        (request.future_read_set.add(k) for k in future_read_set)

        self.get_push_socket.send(request.SerializeToString())

        try:
            msg = self.get_pull_socket.recv()
        except zmq.ZMQError as e:
            if e.errno == zmq.EAGAIN:
                print("request timed out")
                return None
            else:
                print("unknown zmq error")
                return None
        else:
            kv_pairs = {}
            resp = CausalResponse()
            resp.ParseFromString(msg)

            for tp in resp.tuples:
                if tp.error == 1:
                    print('Key %s does not exist!' % (key))
                    return None

                val = CrossCausalValue()
                val.ParseFromString(tp.payload)

                # for now, we just take the first value in the setlattice
                if deserialize[tp.key]:
                    kv_pairs[tp.key] = (val.vector_clock, deserialize_val(val.values[0]))
                else:
                    kv_pairs[tp.key] = (val.vector_clock, val.values[0])
            if len(resp.versioned_keys) != 0:
                return ((resp.versioned_key_query_addr, resp.versioned_keys), kv_pairs)
            else:
                return (None, kv_pairs)

    def put(self, key, value):
        request = KeyRequest()
        request.type = PUT

        tp = request.tuples.add()
        tp.key = key

        if type(value) == LWWPairLattice:
            tp.lattice_type = LWW

            ser = LWWValue()
            ser.timestamp = value.reveal()[0]
            ser.value = value.reveal()[1]

            tp.payload = ser.SerializeToString()
        elif type(value) == SetLattice:
            tp.lattice_type = SET

            ser = SetValue()
            ser.values.extend(list(value.reveal()))

            tp.payload = ser.SerializeToString()
        else:
            raise ValueError('Invalid PUT type: ' + str(type(value)))

        request.response_address = PUT_RESPONSE_ADDR_TEMPLATE % thread_id

        self.put_push_socket.send(request.SerializeToString())

        try:
            msg = self.put_pull_socket.recv()
        except zmq.ZMQError as e:
            if e.errno == zmq.EAGAIN:
                print("request timed out")
                return False
            else:
                print("unknown zmq error")
                return False
        else:
            resp = KeyResponse()
            resp.ParseFromString(msg)

            return resp.tuples[0].error == 0

    def causal_put(self, key, vector_clock, dependency, value, client_id):
        request = CausalRequest()
        request.consistency = CROSS
        request.id = client_id

        tp = request.tuples.add()
        tp.key = key

        cross_causal_value = CrossCausalValue()
        cross_causal_value.vector_clock = vector_clock

        for key in dependency:
            dep = cross_causal_value.deps.add()
            dep.key = key
            dep.vector_clock = dependency[key]

        cross_causal_value.values.add(value)

        tp.payload = SerializeToString(cross_causal_value)

        request.response_address = PUT_RESPONSE_ADDR_TEMPLATE % thread_id

        self.put_push_socket.send(request.SerializeToString())

        try:
            msg = self.put_pull_socket.recv()
        except zmq.ZMQError as e:
            if e.errno == zmq.EAGAIN:
                print("request timed out")
                return False
            else:
                print("unknown zmq error")
                return False
        else:
            return True
