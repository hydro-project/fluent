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

import logging
import os
import random
import socket
import zmq

from .lattices import *
from .common import *
from .zmq_util import *

if not os.path.isfile('kvs_pb2.py'):
    print('You are running in an environment where protobufs were not ' +
            'automatically compiled. Please run protoc before proceeding.')
from .kvs_pb2 import *

class AnnaClient():
    def __init__(self, elb_addr, ip=None, elb_ports=list(range(6450, 6454)), offset=0):
        assert type(elb_addr) == str, \
            'ELB IP argument must be a string.'

        self.elb_addr = elb_addr
        self.elb_ports = elb_ports

        if ip:
            self.ut = UserThread(ip, offset)
        else:
            self.ut = UserThread(socket.gethostbyname(socket.gethostname()), offset)

        self.context = zmq.Context(1)

        self.address_cache = {}
        self.pusher_cache = SocketCache(self.context, zmq.PUSH)

        self.response_puller = self.context.socket(zmq.PULL)
        self.response_puller.bind(self.ut.get_request_pull_bind_addr())

        self.key_address_puller = self.context.socket(zmq.PULL)
        self.key_address_puller.bind(self.ut.get_key_address_bind_addr())

        self.rid = 0

    def get(self, key):
        worker_address = self._get_worker_address(key)

        if not worker_address:
            return None

        send_sock = self.pusher_cache.get(worker_address)

        req, _ = self._prepare_data_request(key)
        req.type = GET

        send_request(req, send_sock)
        response = recv_response([req.request_id], self.response_puller,
                KeyResponse)[0]

        # we currently only support single key operations
        tup = response.tuples[0]

        if tup.invalidate:
            self._invalidate_cache(tup.key, tup.addresses)

        if tup.error == 0:
            return self._deserialize(tup)
        elif tup.error == 1:
            logging.info('key %s does not exist' % key)
            return None # key does not exist
        else:
            return self.get(tup.key) # re-issue the request

    def get_all(self, key):
        worker_addresses = self._get_worker_address(key, False)

        if not worker_addresses:
            return None

        req, _ = self._prepare_data_request(key)
        req.type = GET

        req_ids = []
        for address in worker_addresses:
            # NOTE: We technically waste a request id here, but it doesn't
            # really matter
            req.request_id = self._get_request_id()

            send_sock = self.pusher_cache.get(address)
            send_request(req, send_sock)

            req_ids.append(req.request_id)

        responses = recv_response(req_ids, self.response_puller, KeyResponse)

        for resp in responses:
            tup = resp.tuples[0]
            if tup.invalidate:
                # reissue the request
                self._invalidate_cache(tup.key, tup.addresses)
                return self.get_all(key)

            if tup.error != 0:
                return None

        return list(map(lambda resp: self._deserialize(resp), responses))


    def put_all(self, key, value):
        worker_addresses = self._get_worker_address(key, False)

        if not worker_addresses:
            return False

        req, tup = self._prepare_data_request(key)
        req.type = PUT
        tup.payload, tup.lattice_type = self._serialize(value)
        tup.timestamp = 0

        req_ids = []
        for address in worker_addresses:
            # NOTE: We technically waste a request id here, but it doesn't
            # really matter
            req.request_id = self._get_request_id()

            send_sock = self.pusher_cache.get(address)
            send_request(req, send_sock)

            req_ids.append(req.request_id)

        responses = recv_response(req_ids, self.response_puller, KeyResponse)

        for resp in responses:
            tup = resp.tuples[0]
            if tup.invalidate:
                # reissue the request
                self._invalidate_cache(tup.key, tup.addresses)
                return self.durable_put(key, value)

            if tup.error != 0:
                return False

        return True


    def put(self, key, value):
        worker_address = self._get_worker_address(key)

        if not worker_address:
            return False

        send_sock = self.pusher_cache.get(worker_address)

        req, tup = self._prepare_data_request(key)
        req.type = PUT

        tup.payload, tup.lattice_type = self._serialize(value)

        send_request(req, send_sock)
        response = recv_response([req.request_id], self.response_puller,
                KeyResponse)[0]

        # we currently only support single key operations
        tup = response.tuples[0]

        if tup.invalidate:
            self._invalidate_cache(tup.key, tup.addresses)

            # re-issue the request
            return self.put(tup.key)

        return tup.error == 0

    def _deserialize(self, tup):
        if tup.lattice_type == LWW:
            val = LWWValue()
            val.ParseFromString(tup.payload)

            return LWWPairLattice(val.timestamp, val.value)
        elif tup.lattice_type == SET:
            s = SetValue()
            s.ParseFromString(tup.payload)

            result = set()
            for k in s.keys:
                result.add(k)

            return SetLattice(result)
        elif tup.lattice_type == CROSSCAUSAL:
            res = CrossCausalValue()
            res.ParseFromString(tup.payload)
            return res

    def _serialize(self, val):
        #print("entering serialize")
        if isinstance(val, LWWPairLattice):
            #print("lww")
            lww = LWWValue()
            lww.timestamp = val.ts
            lww.value = val.val
            return lww.SerializeToString(), LWW
        elif isinstance(val, SetLattice):
            #print("set")
            s = SetValue()
            for o in val:
                s.values.append(o)
            return s.SerializeToString(), SET
        elif type(val).__name__ == 'CrossCausalValue':
            #print("causal")
            return val.SerializeToString(), CROSSCAUSAL
        else:
            print("very bad")
            #print(type(val))
            return 123, 456

    def _prepare_data_request(self, key):
        req = KeyRequest()
        req.request_id = self._get_request_id()
        req.response_address = self.ut.get_request_pull_connect_addr()
        tup = req.tuples.add()

        tup.key = key
        tup.address_cache_size = len(self.address_cache[key])

        return (req, tup)


    def _get_request_id(self):
        response = self.ut.get_ip() + ':' + str(self.rid)
        self.rid = (self.rid + 1) % 10000
        return response


    def _get_worker_address(self, key, pick=True):
        if key not in self.address_cache:
            port = random.choice(self.elb_ports)
            addresses = self._query_routing(key, port)
            self.address_cache[key] = addresses

        if len(self.address_cache[key]) == 0:
            return None

        if pick:
            return random.choice(self.address_cache[key])
        else:
            return self.address_cache[key]


    def _invalidate_cache(self, key, new_addresses=None):
        if new_addresses:
            self.address_cache[key] = new_addresses
        else:
            del self.address_cache[key]


    def _query_routing(self, key, port):
        key_request = KeyAddressRequest()

        key_request.response_address = self.ut.get_key_address_connect_addr()
        key_request.keys.append(key)
        key_request.request_id = self._get_request_id()

        dst_addr = 'tcp://' + self.elb_addr  + ':' + str(port)
        send_sock = self.pusher_cache.get(dst_addr)

        send_request(key_request, send_sock)
        response = recv_response([key_request.request_id],
                self.key_address_puller,  KeyAddressResponse)[0]

        if response.error != 0:
            return []

        result = []
        for t in response.addresses:
            if t.key == key:
                for a in t.ips:
                    result.append(a)

        return result
