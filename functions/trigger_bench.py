import cloudpickle as cp
import logging
import sys
import time
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

epoch_total = []
total = []
end_recv = 0

epoch_recv = 0
epoch = 1
epoch_thruput = 0
epoch_start = time.time()

while end_recv < sent_msgs:
    msg = recv_socket.recv()

    if b'END' in msg:
        end_recv += 1
    else:
        msg = cp.loads(msg)

        if type(msg) == tuple:
            epoch_thruput += msg[0]
            new_tot = msg[1]
        else:
            new_tot = msg

        epoch_total += new_tot
        total += new_tot
        epoch_recv += 1

        if epoch_recv == sent_msgs:
            epoch_end = time.time()
            elapsed = epoch_end - epoch_start
            thruput = epoch_thruput / elapsed

            logging.info('\n\n*** EPOCH %d ***' % (epoch))
            logging.info('\tTHROUGHPUT: %.2f' % (thruput))
            utils.print_latency_stats(epoch_total, 'E2E', True)

            epoch_recv = 0
            epoch_thruput = 0
            epoch_total.clear()
            epoch_start = time.time()
            epoch += 1

logging.info('*** END ***')

if len(total) > 0:
    utils.print_latency_stats(total, 'E2E', True)
