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

# Define the number of memory threads
MEMORY_THREAD_NUM = 4

# Define the number of ebs threads
EBS_THREAD_NUM = 4

# Define the number of proxy worker threads
PROXY_THREAD_NUM = 4

# Number of tiers
MIN_TIER = 1
MAX_TIER = 2

# Define port offset
SERVER_PORT = 6560
NODE_JOIN_BASE_PORT = 6660
NODE_DEPART_BASE_PORT = 6760
SELF_DEPART_BASE_PORT = 6860
REPLICATION_FACTOR_BASE_PORT = 6960
REQUEST_PULLING_BASE_PORT = 6460
GOSSIP_BASE_PORT = 7060
REPLICATION_FACTOR_CHANGE_BASE_PORT = 7160

# used by proxies
SEED_BASE_PORT = 6560
NOTIFY_BASE_PORT = 6660
KEY_ADDRESS_BASE_PORT = 6760

# used by monitoring nodes
DEPART_DONE_BASE_PORT = 6760
LATENCY_REPORT_BASE_PORT = 6860

# used by benchmark threads
COMMAND_BASE_PORT = 6560

class Thread():
    def __init__(self, ip, tid):
        self.ip = ip
        self.tid = tid

        self._base = 'tcp://*:'
        self._ip_base = 'tcp://' + self.ip + ':'

    def get_ip(self):
        return self.ip

    def get_tid(self):
        return self.tid


class UserThread(Thread):
    def get_request_pull_connect_addr(self):
        return self._ip_base + str(self.tid + REQUEST_PULLING_BASE_PORT)

    def get_request_pull_bind_addr(self):
        return self._base + str(self.tid + REQUEST_PULLING_BASE_PORT)

    def get_key_address_connect_addr(self):
        return self._ip_base + str(self.tid + KEY_ADDRESS_BASE_PORT)

    def get_key_address_bind_addr(self):
        return self._base + str(self.tid + KEY_ADDRESS_BASE_PORT)
