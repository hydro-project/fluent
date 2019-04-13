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
import time
import zmq

from benchmarks.server import *
from executor.server import *
import client as flclient
from scheduler.server import *

REPORT_THRESH = 30
global_util = 0.0

def run():
    mgmt_ip = os.environ['MGMT_IP']
    ip = os.environ['MY_IP']

    sys_func = os.environ['SYSTEM_FUNC']
    if sys_func == 'scheduler':
        route_addr = os.environ['ROUTE_ADDR']
        scheduler(ip, mgmt_ip, route_addr)
    if sys_func == 'benchmark':
        function_addr = os.environ['FUNCTION_ADDR']
        thread_id = int(os.environ['THREAD_ID'])

        flconn = flclient.FluentConnection(function_addr, ip, thread_id)
        benchmark(flconn, thread_id)
    else:
        schedulers = os.environ['SCHED_IPS'].split(' ')
        thread_id = int(os.environ['THREAD_ID'])
        executor(ip, mgmt_ip, schedulers, thread_id)

if __name__ == '__main__':
    run()
