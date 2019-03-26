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

from anna.lattices import *
from include.shared import *
from include.serializer import *
from include.server_utils import *

FUNCOBJ = 'funcs/index-allfuncs'

def _get_func_list(client, prefix, fullname=False):
    funcs = client.get(FUNCOBJ)
    if not funcs:
        return []

    funcs = default_ser.load(funcs.reveal()[1])

    prefix = FUNC_PREFIX + prefix
    result = list(filter(lambda fn: fn.startswith(prefix), funcs))

    if not fullname:
        result = list(map(lambda fn: fn.split(FUNC_PREFIX)[-1], result))

    return result


def _put_func_list(client, funclist):
    # remove duplicates
    funclist = list(set(funclist))

    l = LWWPairLattice(generate_timestamp(0), default_ser.dump(funclist))
    client.put(FUNCOBJ, l)


def _get_pin_address(ip, tid):
    return 'tcp://' + ip + ':' + str(PIN_PORT + tid)


def _get_unpin_address(ip, tid):
    return 'tcp://' + ip + ':' + str(UNPIN_PORT + tid)


def _get_exec_address(ip, tid):
    return 'tcp://' + ip + ':' + str(EXEC_PORT + tid)

