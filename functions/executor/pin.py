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
import sys

from . import utils
from include.functions_pb2 import *
from include import server_utils as sutils

def pin(pin_socket, pusher_cache, client, status, pinned_functions, runtimes,
        exec_counts):
    msg = pin_socket.recv_string()
    splits = msg.split(':')

    resp_ip, name = splits[0], splits[1]
    sckt = pusher_cache.get(sutils._get_pin_accept_port(resp_ip))

    if (sutils.ISOLATION == 'STRONG' and len(pinned_functions) > 0) or not \
            status.running:
            resp = sutils.error.SerializeToString()
            sckt.send(sutils.error.SerializeToString())
            return

    logging.info('Adding function %s to my local pinned functions.' % (name))
    sckt.send(sutils.ok_resp)

    func = utils._retrieve_function(name, client)

    # the function must exist -- because otherwise the DAG couldn't be
    # registered -- so we keep trying to retrieve it
    while not func:
        func = utils._retrieve_function(name, client)

    status.functions.append(name)
    pinned_functions[name] = func
    runtimes[name] = 0.0
    exec_counts[name] = 0

def unpin(unpin_socket, status, pinned_functions, runtimes, exec_counts):
    name = unpin_socket.recv_string() # the name of the func to unpin
    logging.info('Removing function %s from my local pinned functions.' %
            (name))

    # we restart the container to clear all global state
    if isolation == 'STRONG':
        sys.exit(0)

    # we don't have the function pinned, we can just ignore this
    if name not in status.functions:
        return

    func_queue = queue[name]
    # if there are no currently pending requests, then we can simply
    # unpin the existing function
    if len(func_queue) == 0:
        del pinned_functions[name]
        del queue[name]
        del runtimes[name]
        del exec_counts[name]

    status.functions.remove(name)
