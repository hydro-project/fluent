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
import kubernetes as k8s
import random
from util import *

ec2_client = boto3.client('ec2', 'us-east-1')

def add_nodes(client, cfile, kinds, counts, mon_ips, route_ips=[], node_ips=[],
        route_addr=None, scheduler_ips=[], function_addr=None):
    if node_ips:
        assert len(kinds) == len(counts) == len(node_ips), ('Must have same ' +
                'number of kinds and counts and node_ips.')
    else:
        assert len(kinds) == len(counts), ('Must have same number of kinds and '
                + 'counts.')

    cluster_name = check_or_get_env_arg('NAME')

    prev_counts = []
    for i in range(len(kinds)):
        print('Adding %d %s server node(s) to cluster...' % (counts[i], kinds[i]))
        # get the previous number of nodes of type kind that are running
        prev_count = get_previous_count(client, kinds[i])
        prev_counts.append(prev_count)

        # we only add new nodes if we didn't pass in a node IP
        if not node_ips:
            # run kops script to add servers to the cluster
            run_process(['./modify_ig.sh', kinds[i], str(counts[i] + prev_count)])

    run_process(['./validate_cluster.sh'])

    kops_ip = get_pod_ips(client, 'role=kops')[0]

    route_str = ' '.join(route_ips)
    mon_str = ' '.join(mon_ips)
    sched_str = ' '.join(scheduler_ips)

    for i in range(len(kinds)):
        kind = kinds[i]

        # select the newest nodes if we don't have any preallocated nodes
        if not node_ips:
            role_selector = 'role=%s' % (kind)
            new_nodes = client.list_node(label_selector=role_selector).items
            new_nodes.sort(key=lambda node: node.metadata.creation_timestamp,
                    reverse=True)
        else: # otherwise just use the nodes we have
            new_nodes = node_ips[i]

        if prev_counts[i] > 0:
            role_selector = 'role=%s' % (kind)
            max_id = max(list(map(lambda pod:
                int(pod.spec.node_selector['podid'].split('-')[-1]),
                client.list_namespaced_pod(namespace=NAMESPACE, label_selector=\
                        role_selector).items)))
        else:
            max_id = 0

        index = 0
        for j in range(max_id + 1, max_id + counts[i] + 1):
            podid = '%s-%d' % (kind, j)
            client.patch_node(new_nodes[index].metadata.name, body={'metadata': {'labels':
                {'podid': podid}}})
            index += 1

        created_pods = []
        print('Creating %d %s pod(s)...' % (counts[i], kind))
        for j in range(max_id + 1, max_id + counts[i] + 1):
            filename = 'yaml/pods/%s-pod.yml' % (kind)
            pod_spec = load_yaml(filename)
            pod_name = '%s-pod-%d' % (kind, j)

            seed_ip = random.choice(route_ips) if route_ips else ''

            pod_spec['metadata']['name'] = pod_name
            for container in pod_spec['spec']['containers']:
                env = container['env']
                replace_yaml_val(env, 'ROUTING_IPS', route_str)
                replace_yaml_val(env, 'SCHED_IPS', sched_str)
                replace_yaml_val(env, 'ROUTE_ADDR', route_addr)
                replace_yaml_val(env, 'FUNCTION_ADDR', function_addr)
                replace_yaml_val(env, 'MON_IPS', mon_str)
                replace_yaml_val(env, 'MGMT_IP', kops_ip)
                replace_yaml_val(env, 'SEED_IP', seed_ip)

                created_pods += [(pod_name, container['name'])]

            pod_spec['spec']['nodeSelector']['podid'] = ('%s-%d' % (kind, j))

            if kind == 'ebs':
                vols = pod_spec['spec']['volumes']
                for i in range(EBS_VOL_COUNT):
                    volobj = ec2_client.create_volume(
                            AvailabilityZone='us-east-1a', Size=64,
                            VolumeType='gp2')
                    volid = volobj['VolumeId']

                    ec2_client.create_tags(Resources=[volid], Tags=[
                        {
                            'Key': 'KubernetesCluster',
                            'Value': cluster_name
                        }
                    ])

                    vols[i]['awsElasticBlockStore']['volumeID'] = volid

            client.create_namespaced_pod(namespace=NAMESPACE,
                    body=pod_spec)

            # wait until all pods of this kind are running
            ips = get_pod_ips(client, 'role='+kind, isRunning=True)

            os.system('cp %s ./kvs-config.yml' % cfile)
            for pname, cname in created_pods:
                copy_file_to_pod(client, 'kvs-config.yml', pname,
                        '/fluent/conf/', cname)

            os.system('rm ./kvs-config.yml')
