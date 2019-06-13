#!/usr/bin/env python3.6

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

import boto3
import random
import os

import util

ec2_client = boto3.client('ec2', os.getenv('AWS_REGION', 'us-east-1'))


def add_nodes(client, apps_client, cfile, kinds, counts, create=False):
    for i in range(len(kinds)):
        print('Adding %d %s server node(s) to cluster...' %
              (counts[i], kinds[i]))

        # get the previous number of nodes of type kind that are running
        prev_count = util.get_previous_count(client, kinds[i])

        # we only add new nodes if we didn't pass in a node IP
        util.run_process(['./modify_ig.sh', kinds[i],
                          str(counts[i] + prev_count)])

    util.run_process(['./validate_cluster.sh'])

    kops_ip = util.get_pod_ips(client, 'role=kops')[0]
    route_ips = util.get_pod_ips(client, 'role=routing')
    if len(route_ips) > 0:
        seed_ip = random.choice(route_ips)
    else:
        seed_ip = ''

    mon_str = ' '.join(util.get_pod_ips(client, 'role=monitoring'))
    route_str = ' '.join(route_ips)
    sched_str = ' '.join(util.get_pod_ips(client, 'role=scheduler'))

    route_addr = util.get_service_address(client, 'routing-service')
    function_addr = util.get_service_address(client, 'function-service')

    # create should only be true when the DaemonSet is being created for the
    # first time -- i.e., when this is called from create_cluster
    if create:
        for i in range(len(kinds)):
            kind = kinds[i]

            fname = 'yaml/ds/%s-ds.yml' % kind
            yml = util.load_yaml(fname)

            for container in yml['spec']['template']['spec']['containers']:
                env = container['env']

                util.replace_yaml_val(env, 'ROUTING_IPS', route_str)
                util.replace_yaml_val(env, 'ROUTE_ADDR', route_addr)
                util.replace_yaml_val(env, 'SCHED_IPS', sched_str)
                util.replace_yaml_val(env, 'FUNCTION_ADDR', function_addr)
                util.replace_yaml_val(env, 'MON_IPS', mon_str)
                util.replace_yaml_val(env, 'MGMT_IP', kops_ip)
                util.replace_yaml_val(env, 'SEED_IP', seed_ip)

            apps_client.create_namespaced_daemon_set(namespace=util.NAMESPACE,
                                                     body=yml)

            # wait until all pods of this kind are running
            res = []
            while len(res) != counts[i]:
                res = util.get_pod_ips(client, 'role='+kind, is_running=True)

            created_pods = []
            pods = client.list_namespaced_pod(namespace=util.NAMESPACE,
                                              label_selector='role=' +
                                              kind).items
            for pod in pods:
                pname = pod.metadata.name
                for container in pod.spec.containers:
                    cname = container.name
                    created_pods.append((pname, cname))

            os.system('cp %s ./kvs-config.yml' % cfile)
            for pname, cname in created_pods:
                util.copy_file_to_pod(client, 'kvs-config.yml', pname,
                                      '/fluent/conf/', cname)

            os.system('rm ./kvs-config.yml')
