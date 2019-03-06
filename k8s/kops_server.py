#!/usr/bin/env python3

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

from add_nodes import add_nodes
import logging
from misc_pb2 import *
from requests_pb2 import *
import os
import random
from remove_node import remove_node
import subprocess
import time
import util
import zmq

logging.basicConfig(filename='log.txt',level=logging.INFO)
THRESHOLD = 30

def run():
    context = zmq.Context(1)
    restart_pull_socket = context.socket(zmq.REP)
    restart_pull_socket.bind('tcp://*:7000')

    churn_pull_socket = context.socket(zmq.PULL)
    churn_pull_socket.bind('tcp://*:7001')

    extant_caches_socket = context.socket(zmq.REP)
    extant_caches_socket.bind('tcp://*:7002')

    poller = zmq.Poller()
    poller.register(restart_pull_socket, zmq.POLLIN)
    poller.register(churn_pull_socket, zmq.POLLIN)
    poller.register(extant_caches_socket, zmq.POLLIN)

    cfile = '/fluent/conf/kvs-base.yml'

    # waits until the kubecfg file gets copied into the pod -- this might be
    # brittle if we try to move to a non-Ubuntu setting, but I'm not worried
    # about that for now
    while not os.path.isfile('/root/.kube/config'):
        pass

    client = util.init_k8s()

    start = time.time()
    while True:
        socks = dict(poller.poll(timeout=1000))

        if churn_pull_socket in socks and socks[churn_pull_socket] == \
                zmq.POLLIN:

            msg = churn_pull_socket.recv_string()
            args = msg.split(':')

            if args[0] == 'add':
                num = int(args[1])
                ntype = args[2]
                logging.info('Adding %d new %s node(s)...' % (num, ntype))

                if len(args) > 3:
                    num_threads = args[3]
                else:
                    num_threads = 3

                mon_ips = util.get_pod_ips(client, 'role=monitoring')
                route_ips = util.get_pod_ips(client, 'role=routing')

                os.system('sed -i "s|%s: [0-9][0-9]*|%s: %d|g" %s' % (ntype,
                    ntype, num_threads, cfile))
                os.system('sed -i "s|%s-cap: [0-9][0-9]*|%s: %d|g" %s' % (ntype,
                    ntype, num_threads * 15, cfile))

                add_nodes(client, cfile, [ntype], [num], mon_ips, route_ips)
                logging.info('Successfully added %d %s node(s).' % (num, ntype))
            elif args[0] == 'remove':
                ip = args[1]
                ntype = args[2]

                remove_node(ip, ntype)
                logging.info('Successfully removed node %s.' % (ip))

        if restart_pull_socket in socks and socks[restart_pull_socket] == \
                zmq.POLLIN:

            msg = restart_pull_socket.recv_string()
            args = msg.split(':')

            ip = args[1]
            pod = util.get_pod_from_ip(client, ip)

            count = str(pod.status.container_statuses[0].restart_count)

            logging.info('Returning restart count ' + count + ' for IP ' + ip + '.')
            restart_pull_socket.send_string(count)

        if extant_caches_socket in socks and socks[extant_caches_socket] == \
                zmq.POLLIN:

            # It doesn't matter what is in this message
            msg = extant_caches_socket.recv_string()

            ks = KeySet()
            for ip in util.get_pod_ips(clinet, 'role=function'):
                ks.add_keys(ip)

            extant_caches_socket.send_string(ks.SerializeToString())

        end = time.time()
        if end - start > THRESHOLD:
            logging.info('Checking hash ring...')
            check_hash_ring(client, context)

            logging.info('Checking for extra nodes...')
            check_unused_nodes(client)

            start = time.time()

def check_hash_ring(client, context):
    # get routing IPs
    route_ips = util.get_pod_ips(client, 'role=routing')

    if not route_ips:
        return

    ip = random.choice(route_ips)

    route_addr_port = 6350
    storage_depart_port = 6050
    route_depart_port = 6400
    mon_depart_port = 6600

    # bind to routing port
    socket = context.socket(zmq.REQ)
    socket.connect('tcp://' + ip + ':' + str(route_addr_port))

    # get all nodes that are members
    socket.send_string('')
    resp = socket.recv()

    tm = TierMembership()
    tm.ParseFromString(resp)
    tier_data = tm.tiers

    if len(tier_data) == 0:
        return
    elif len(tier_data) > 1:
        mem_tier, ebs_tier = tier_data[0], tier_data[1] if tier_data[0].tier_id \
                == 1 else tier_data[1], tier_data[0]
    else:
        mem_tier, ebs_tier = tier_data[0], None

    # check memory tier
    mem_ips = util.get_pod_ips(client, 'role=memory')

    departed = []
    for node in mem_tier.servers:
        if node.private_ip not in mem_ips:
            departed.append(('1', node))

    # check EBS tier
    ebs_ips = []
    if ebs_tier:
        ebs_ips = util.get_pod_ips(client, 'role=ebs')
        for node in ebs_tier.servers:
            if node.private_ip not in ebs_ips:
                ebs_departed.append(('2', node))

    mon_ips = util.get_pod_ips(client, 'role=monitoring')
    storage_ips = mem_ips + ebs_ips

    logging.info('Found %d departed nodes.' % (len(departed)))
    for pair in departed:
        logging.info('Informing cluster that node %s/%s has departed.' %
                (pair[1].public_ip, pair[1].private_ip))
        msg = pair[0] + ':' + pair[1].public_ip + ':' + pair[1].private_ip
        for ip in storage_ips:
            send_msg(msg, context, ip, storage_depart_port)

        msg = 'depart:' + msg
        for ip in route_ips:
            send_msg(msg, context, ip, route_depart_port)

        for ip in mon_ips:
            send_msg(msg, context, ip, mon_depart_port)

def send_msg(msg, context, ip, port):
    sckt = context.socket(zmq.PUSH)
    sckt.connect('tcp://' + ip + ':' + str(port))
    sckt.send_string(msg)

def check_unused_nodes(client):
    kinds = ['ebs', 'memory']

    for kind in kinds:
        selector = 'role=' + kind

        nodes = {}
        for node in client.list_node(label_selector=selector).items:
            ip = list(filter(lambda address: address.type == 'InternalIP',
                    node.status.addresses))[0].address
            nodes[ip] = node

        node_ips = nodes.keys()

        pod_ips = util.get_pod_ips(client, selector)
        unallocated = set(node_ips) - set(pod_ips)

        mon_ips = util.get_pod_ips(client, 'role=monitoring')
        route_ips = util.get_pod_ips(client, 'role=routing')

        logging.info('Found %d unallocated %s nodes.' % (len(unallocated),
            kind))
        for node_ip in unallocated:
            # note that the last argument is a list of lists
            add_nodes(client, [kind], [1], mon_ips, route_ips, [[nodes[node_ip]]])

if __name__ == '__main__':
    # wait for this file to appear before starting
    while not os.path.isfile('/fluent/setup_complete'):
        pass

    run()
