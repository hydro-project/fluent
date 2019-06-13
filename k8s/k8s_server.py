#!/usr/bin/env python3

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
import zmq

from add_nodes import add_nodes
from remove_node import remove_node
import util

logging.basicConfig(filename='log_k8s.txt', level=logging.INFO)


def run():
    context = zmq.Context(1)
    client, apps_client = util.init_k8s()

    node_add_socket = context.socket(zmq.PULL)
    node_add_socket.bind('ipc:///tmp/node_add')

    node_remove_socket = context.socket(zmq.PULL)
    node_remove_socket.bind('ipc:///tmp/node_remove')

    poller = zmq.Poller()
    poller.register(node_add_socket, zmq.POLLIN)
    poller.register(node_remove_socket, zmq.POLLIN)

    cfile = '/fluent/conf/kvs-base.yml'

    while True:
        socks = dict(poller.poll(timeout=1000))

        if node_add_socket in socks and socks[node_add_socket] == zmq.POLLIN:
            msg = node_add_socket.recv_string()
            args = msg.split(':')

            ntype = args[0]
            num = int(args[1])
            logging.info('Adding %d new %s node(s)...' % (num, ntype))

            add_nodes(client, apps_client, cfile, [ntype], [num])
            logging.info('Successfully added %d %s node(s).' % (num, ntype))

        if node_remove_socket in socks and socks[node_remove_socket] == \
                zmq.POLLIN:
            msg = node_remove_socket.recv_string()
            args = msg.split(':')

            ntype = args[0]
            ip = args[1]

            remove_node(ip, ntype)
            logging.info('Successfully removed node %s.' % (ip))


if __name__ == '__main__':
    # wait for this file to appear before starting
    while not os.path.isfile('/fluent/setup_complete'):
        pass

    run()
