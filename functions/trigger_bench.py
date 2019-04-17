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

sckt = ctx.socket(zmq.PUSH)
sckt.connect('tcp://' + ips[0] + ':3000')

sckt.send_string(msg)
msg = recv_socket.recv()

logging.info("%s" % msg)