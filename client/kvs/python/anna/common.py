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

class ProxyThread(Thread):
    def get_key_address_connect_addr(self):
        return self._ip_base + str(self.tid + KEY_ADDRESS_BASE_PORT)

    def key_address_bind_addr(self):
        return self._base + str(self.tid + KEY_ADDRESS_BASE_PORT)
