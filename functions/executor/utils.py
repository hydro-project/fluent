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

from include.functions_pb2 import *
from include.kvs_pb2 import *
from include import server_utils

# create generic error response
error = GenericResponse()
error.success = False

# create generic OK response
ok = GenericResponse()
ok.success = True
OK_RESP = ok.SerializeToString()

def _retrieve_function(name, kvs):
    kvs_name = server_utils._get_func_kvs_name(name)
    latt = kvs.get(kvs_name, LWW)

    if latt:
        return function_ser.load(latt.value)
    else:
        return None


def _push_status(schedulers, status):
    msg = status.SerializeToString()

    # tell all the schedulers your new status
    for sched in schedulers:
        sckt = ctx.socket(zmq.PUSH)
        sckt.connect(_get_status_ip(sched))
        sckt.send_string(msg)

