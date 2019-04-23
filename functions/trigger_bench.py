import cloudpickle as cp
import logging
import sys
import zmq

from benchmarks import utils

logging.basicConfig(filename='log_trigger.txt', level=logging.INFO, format='%(asctime)s %(message)s')

NUM_THREADS = 4

ips = []
with open('bench_ips.txt', 'r') as f:
    l = f.readline()
    while l:
        ips.append(l.strip())
        l = f.readline()

for ip in ips:
	print(ip)

msg = sys.argv[1]
ctx = zmq.Context(1)

recv_socket = ctx.socket(zmq.PULL)
recv_socket.bind('tcp://*:3000')

sent_msgs = 0

if 'create' in msg:
    sckt = ctx.socket(zmq.PUSH)
    sckt.connect('tcp://' + ips[0] + ':3000')

    sckt.send_string(msg)
    sent_msgs += 1
elif 'warmup' in msg:
	index = 0
	for ip in ips:
		for tid in range(NUM_THREADS):
			sckt = ctx.socket(zmq.PUSH)
			sckt.connect('tcp://' + ip + ':' + str(3000 + tid))
			sckt.send_string(msg + ':' + str(index))
			sent_msgs += 1
			index += 1
elif 'run' in msg:
	index = 0
	for ip in ips:
		for tid in range(NUM_THREADS):
			sckt = ctx.socket(zmq.PUSH)
			sckt.connect('tcp://' + ip + ':' + str(3000 + tid))
			sckt.send_string(msg + ':' + str(index))
			sent_msgs += 1
			index += 1

end_recv = 0

latency = {}
latency[1] = []
latency[2] = []
latency[3] = []
latency[4] = []

while end_recv < sent_msgs:
	payload = recv_socket.recv()
	logging.info("received response")
	end_recv += 1
	if 'run' in msg:
		bench_latency = cp.loads(payload)
		for length in bench_latency:
			latency[length] += bench_latency[length]

if 'run' in msg:
	for length in latency:
		logging.info("length %d" % length)
		utils.print_latency_stats(latency[length], 'Causal', True)

logging.info("benchmark done")