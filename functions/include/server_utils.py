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
FUNC_PREFIX = 'funcs/'
BIND_ADDR_TEMPLATE = 'tcp://*:%d'

PIN_PORT = 4000
UNPIN_PORT = 4010
FUNC_EXEC_PORT = 4020
DAG_QUEUE_PORT = 4030
DAG_EXEC_PORT = 4040
SELF_DEPART_PORT = 4050

STATUS_PORT = 5007
SCHED_UPDATE_PORT = 5008
BACKOFF_PORT = 5009
PIN_ACCEPT_PORT = 5010

# For message sending via the user library.
RECV_INBOX_PORT = 5500

STATISTICS_REPORT_PORT = 7006

# create generic error response
error = GenericResponse()
error.success = False

# create generic OK response
ok = GenericResponse()
ok.success = True
ok_resp = ok.SerializeToString()


def _get_func_kvs_name(fname):
    return FUNC_PREFIX + fname


def _get_dag_trigger_address(ip_tid):
    ip, tid = ip_tid.split(':')
    return 'tcp://' + ip + ':' + str(int(tid) + DAG_EXEC_PORT)


def _get_statistics_report_address(mgmt_ip):
    return 'tcp://' + mgmt_ip + ':' + str(STATISTICS_REPORT_PORT)


def _get_backoff_addresss(ip):
    return 'tcp://' + ip + ':' + str(BACKOFF_PORT)


def _get_pin_accept_port(ip):
    return 'tcp://' + ip + ':' + str(PIN_ACCEPT_PORT)


def _get_dag_predecessors(dag, fname):
    result = []

    for connection in dag.connections:
        if connection.sink == fname:
            result.append(connection.source)

    return result


def _get_user_msg_inbox_addr(ip, tid):
    return 'tcp://' + ip + ':' + str(int(tid) + RECV_INBOX_PORT)
