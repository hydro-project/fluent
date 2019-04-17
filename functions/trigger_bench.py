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

while end_recv < sent_msgs:
	msg = recv_socket.recv()
	logging.info("received response")
	logging.info("%s" % msg)
	end_recv += 1

logging.info("benchmark done")