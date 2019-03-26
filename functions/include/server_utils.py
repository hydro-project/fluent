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

from .functions_pb2 import *

# shared constants
FUNCOBJ = 'funcs/index-allfuncs'
FUNC_PREFIX = 'funcs/'
BIND_ADDR_TEMPLATE = 'tcp://*:%d'

PIN_PORT = 4000
UNPIN_PORT = 4010
FUNC_EXEC_PORT = 4020
DAG_QUEUE_PORT = 4030
DAG_EXEC_PORT = 4040

STATUS_PORT = 5006
SCHED_UPDATE_PORT = 5007

NODES_PORT = 7002
SCHEDULERS_PORT = 7004

# create generic error response
error = GenericResponse()
error.success = False

# create generic OK response
ok = GenericResponse()
ok.success = True
ok_resp = ok.SerializeToString()

def _get_func_kvs_name(fname):
    return FUNC_PREFIX + fname


