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
else:
    for ip in ips:
        for tid in range(NUM_THREADS):
            sckt = ctx.socket(zmq.PUSH)
            sckt.connect('tcp://' + ip + ':' + str(3000 + tid))

            sckt.send_string(msg)
            sent_msgs += 1

total = []
end_recv = 0

epoch_recv = 0
epoch = 1
while end_recv < sent_msgs:
    msg = recv_socket.recv()

    if b'END' in msg:
        end_recv += 1
    else:
        new_tot = cp.loads(msg)
        total += new_tot
        epoch_recv += 1

        if epoch_recv == sent_msgs:
            logging.info('\n\n*** EPOCH %d ***' % (epoch))
            utils.print_latency_stats(total, 'E2E', True)

            epoch_recv = 0
            total.clear()
            epoch += 1
