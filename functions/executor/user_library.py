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

import queue
import threading
import uuid
import zmq

from anna.functions_pb2 import *
# from anna.kvs_pb2 import *
from anna.zmq_util import SocketCache
from include import server_utils

class AbstractFluentUserLibrary:
    # Stores a lattice value at ref.
    def put(self, ref, ltc):
        raise NotImplementedError

    # Retrives the lattice value at ref.
    def get(self, ref, ltype):
        raise NotImplementedError

    # Sends a bytestring message to the specified destination.
    # TODO: type and format for destination ID?
    def send(self, dest, bytestr):
        raise NotImplementedError

    # Receives messages sent by send() to this function.
    # Receives all outstanding messages as a list [(sender id, bytestring message), ...]
    def recv(self):
        raise NotImplementedError

class FluentUserLibrary(AbstractFluentUserLibrary):

    # ip: Executor IP.
    # tid: Executor thread ID.
    # anna_client: The Anna client, used for interfacing with the kvs.
    def __init__(self, ip, tid, anna_client):
        self.ctx = zmq.Context()
        self.send_socket_cache = SocketCache(self.ctx, zmq.PUSH)

        self.executor_ip = ip
        self.executor_tid = tid
        self.client = anna_client

        # Threadsafe queue to serve as this node's inbox.
        # Items are (sender string, message bytestring).
        # NB: currently unbounded in size.
        self.recv_inbox = queue.Queue()

        # Thread for receiving messages into our inbox.
        self.recv_inbox_thread = threading.Thread(target=self._recv_inbox_listener)
        self.recv_inbox_thread.start()

    # We no longer support lattice types in put;
    # everything is append to a set.
    def put(self, ref, values):
        client_id = str(int(uuid.uuid4()))
        vector_clock = {client_id: 1}
        dependency = {}
        return self.causal_put(ref, vector_clock, dependency, values, client_id)

    def causal_put(self, key, vector_clock, dependency, values, client_id):
        return self.client.causal_put(key, vector_clock, dependency, values, client_id)

    def get(self, ref):
        vc, values = self.causal_get(ref, str(int(uuid.uuid4())))
        return values

    # User library interface to causal_get.
    def causal_get(self, ref, client_id):
        _, results = self.client.causal_get(
            ref, set(), {}, CROSS, client_id)
        vc, values = results[ref]
        return vc, values

    # dest is currently (IP string, thread id int) of destination executor.
    def send(self, dest, bytestr):
        ip, tid = dest
        dest_addr = server_utils._get_user_msg_inbox_addr(ip, tid)
        sender = (self.executor_ip, self.executor_tid)

        socket = self.send_socket_cache.get(dest_addr)
        socket.send_pyobj((sender, bytestr))

    def recv(self):
        res = []
        while True:
            try:
                (sender, msg) = self.recv_inbox.get()
                res.append((sender, msg))
            except queue.Empty:
                break
        return res


    # Function that continuously listens for send()s sent by other nodes,
    # and stores the messages in an inbox.
    def _recv_inbox_listener(self):
        # Socket for receiving send() messages from other nodes.
        recv_inbox_socket = self.ctx.socket(zmq.PULL)
        recv_inbox_socket.bind(server_utils.BIND_ADDR_TEMPLATE % (server_utils.RECV_INBOX_PORT + self.executor_tid))

        while True:
            (sender, msg) = recv_inbox_socket.recv_pyobj(0)  # Blocking.
            self.recv_inbox.put((sender, msg))
