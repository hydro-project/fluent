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

from utils import *

def pin(pin_socket, client, status, pinned_functions):
    name = pin_socket.recv_string()
    func = _retrieve_function(name, client)

    # we send an error if we can't retrieve the requested function
    if not func:
        ERROR.error = FUNC_NOT_FOUND
        pin_socket.send(ERROR.SerializeToString())
        continue

    pin_socket.send_string(OK_RESP)

    status.functions[name] = PINNED
    pinned_functions[name] = func
    _push_status(schedulers, status)

def unpin(unpin_socket, status, pinned_functions):
    name = unpin_socket.recv_string() # the name of the func to unpin

    if status.functions[name] != PINNED:
        error.error = NOT_PINNED
        unpin_socket.send(error.SerializeToString())
        continue

    unpin_socket.send(ok_resp)

    func_queue = queue[name]
    # if there are no currently pending requests, then we can simply
    # unpin the existing function
    if len(func_queue) == 0:
        del pinned_functions[name]
        del status.functions[name]
        _push_status(schedulers, status)
    else: # otherwise, we don't accept new requests and wait for the
          # queue to drain
        status.functions[name] = CLEARING
