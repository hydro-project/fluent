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

        context = zmq.Context(1)

        self.address_cache = {}
        self.pusher_cache = SocketCache(context, zmq.PUSH)

        self.response_puller = context.socket(zmq.PULL)
        self.response_puller.bind(self.ut.get_request_pull_bind_addr())

        self.key_address_puller = context.socket(zmq.PULL)
        self.key_address_puller.bind(self.ut.get_key_address_bind_addr())

        self.rid = 0

    def get(self, key):
        worker_address = self._get_worker_address(key)
        send_sock = self.pusher_cache.get(worker_address)

        req, _ = self._prepare_data_request(key)
        req.type = GET

        resp_obj = KeyResponse()

        # TODO: doesn't support invalidate yet
        send_request(req, resp_obj, send_sock, self.response_puller)

        return resp_obj.tuples[0].value


    def put(self, key, value):
        worker_address = self._get_worker_address(key)
        send_sock = self.pusher_cache.get(worker_address)

        req, tup = self._prepare_data_request(key)
        req.type = PUT

        if type(value) == str:
            value = bytes(value, 'utf-8')

        tup.value = value
        tup.timestamp = 0
        resp_obj = KeyResponse()

        # TODO: doesn't support invalidate yet
        send_request(req, resp_obj, send_sock, self.response_puller)

        return resp_obj.tuples[0].error == 0



    def _prepare_data_request(self, key):
        req = KeyRequest()
        req.request_id = self.ut.get_ip() + ':' + str(self.rid)
        req.response_address = self.ut.get_request_pull_connect_addr()
        tup = req.tuples.add()

        tup.key = key
        tup.address_cache_size = len(self.address_cache[key])

        return (req, tup)

    def _get_worker_address(self, key):
        if key not in self.address_cache:
            port = random.choice(self.elb_ports)
            addresses = self._query_proxy(key, port)
            self.address_cache[key] = addresses

        return random.choice(self.address_cache[key])

    def _query_proxy(self, key, port):
        key_request = KeyAddressRequest()

        key_request.response_address = self.ut.get_key_address_connect_addr()
        key_request.keys.append(key)
        key_request.request_id = self.ut.get_ip() + ':' + str(self.rid)
        self.rid += 1

        dst_addr = 'tcp://' + self.elb_addr  + ':' + str(port)
        send_sock = self.pusher_cache.get(dst_addr)
        resp = KeyAddressResponse()

        send_request(key_request, resp, send_sock, self.key_address_puller)

        result = []
        for t in resp.addresses:
            if t.key == key:
                for a in t.ips:
                    result.append(a)

        return result
