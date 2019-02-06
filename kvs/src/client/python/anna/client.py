import random
import socket
import zmq

from .common import *
from .zmq_util import *
from .requests_pb2 import *

class AnnaClient():
    def __init__(self, elb_addr, ip=None, elb_ports=list(range(6000, 6004)), offset=0):
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
        send_sock = self.pusher_cache.get(worker_address)

        req, _ = self._prepare_data_request(key)
        req.type = GET

        send_request(req, send_sock)
        response = recv_response([req.request_id], KeyResponse,
                self.response_puller)[0]

        # we currently only support single key operations
        tup = resp_obj.tuples[0]

        if tup.invalidate:
            self._invalidate_cache(tup.key, tup.addresses)

            # re-issue the request
            return self.get(tup.key)

        return str(tup.value, 'utf-8')

    def durable_put(self, key, value):
        worker_addresses = self._get_worker_address(key, False)

        req, tup = self._prepare_data_request(key)
        req.type = PUT
        tup.value = bytes(value, 'utf-8')
        tup.timestamp = 0

        req_ids = []
        for address in worker_addresses:
            # NOTE: We technically waste a request id here, but it doesn't
            # really matter
            req.request_id = self._get_request_id()

            send_sock = self.pusher_cache.get(worker_address)
            send_request(req, send_sock)

            req_ids.append(req.request_id)

        responses = recv_response(req_ids, self.response_puller, KeyResponse)

        for resp in responses:
            if resp.tuple[0].error != 0:
                return False

        return True


    def put(self, key, value):
        worker_address = self._get_worker_address(key)
        send_sock = self.pusher_cache.get(worker_address)

        req, tup = self._prepare_data_request(key)
        req.type = PUT
        tup.value = bytes(value, 'utf-8')
        tup.timestamp = 0

        send_request(req, send_sock)
        response = recv_response([req.request_id], KeyResponse,
            self.response_puller)[0]

        # we currently only support single key operations
        tup = response.tuples[0]

        if tup.invalidate:
            self._invalidate_cache(tup.key, tup.addresses)

            # re-issue the request
            return self.put(tup.key)

        return tup.error == 0


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
        response = recv_response([key_request.request_id], KeyAddressResponse,
                self.key_address_puller)[0]

        result = []
        for t in response.addresses:
            if t.key == key:
                for a in t.ips:
                    result.append(a)

        return result
