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

import os
import subprocess
import logging
import zmq

logging.basicConfig(filename='log.txt',level=logging.INFO)

def run():
    context = zmq.Context(1)
    request_pull_socket = context.socket(zmq.REP)
    request_pull_socket.bind('tcp://*:7000')

    while True:
        msg = request_pull_socket.recv_string()
        args = msg.split(':')

        if args[0] == 'add':
            num = int(args[1])
            ntype = args[2]
            logging.info('Adding %d new %s nodes...' % (num, ntype))

            if ntype == 'memory':
                resp = os.system('./add_nodes.sh ' + str(num) + ' 0 0 0')
            else:
                resp = os.system('./add_nodes.sh 0 ' + str(num) + ' 0 0')

            if resp == 0:
                logging.info('Successfully added %d %s nodes.' % (num, ntype))
            else:
                logging.error('Unexpected error while adding nodes.')
        elif args[0] == 'remove':
            ip = args[1]
            ntype = args[2]
            logging.info('Removing %s node with IP %s.' % (ntype, ip))

            resp = os.system('./remove_node.sh %s %s' % (ntype, ip))

            if resp == 0:
                logging.info('Successfully removed nodes %s.' % (ip))
            else:
                logging.error('Unexpected error while removing node %s.' % (ip))

        elif args[0] == 'restart':
            ip = args[1]
            count = subprocess.check_output('./get_restart_count.sh ' + ip,
                    shell=True)
            logging.info('Returning restart count ' + str(count, 'utf-8') + ' for IP ' + ip + '.')
            request_pull_socket.send_string(str(count, 'utf-8'))
        else:
            logging.info('Unknown argument type: %s.' % (args[0]))

run()
