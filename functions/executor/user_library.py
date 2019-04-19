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

import cloudpickle as cp
import logging
import queue
import threading
import zmq

from anna.zmq_util import SocketCache
from include import server_utils
from include import shared

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
        self.recv_inbox_thread.do_run = True
        self.recv_inbox_thread.start()

    def put(self, ref, ltc):
        res = self.client.put(ref, ltc)
        return res

    def get(self, ref):
        return self.client.get(ref)[ref]

    def getid(self):
        return (self.executor_ip, self.executor_tid)

    # dest is currently (IP string, thread id int) of destination executor.
    def send(self, dest, bytestr):
        ip, tid = dest
        dest_addr = server_utils._get_user_msg_inbox_addr(ip, tid)
        sender = (self.executor_ip, self.executor_tid)

        socket = self.send_socket_cache.get(dest_addr)
        socket.send_pyobj((sender, bytestr))

    def close(self):
        self.recv_inbox_thread.do_run = False
        self.recv_inbox_thread.join()

    def recv(self):
        res = []
        while True:
            try:
                (sender, msg) = self.recv_inbox.get(block=False)
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
        t = threading.currentThread()

        while t.do_run:
            try:
                (sender, msg) = recv_inbox_socket.recv_pyobj(zmq.NOBLOCK)
                self.recv_inbox.put((sender, msg))
            except zmq.ZMQError as e:
                if e.errno == zmq.EAGAIN:
                    continue
                else:
                    raise e
