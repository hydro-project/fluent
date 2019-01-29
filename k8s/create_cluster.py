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

from add_nodes import add_nodes
import boto3
import json
import kubernetes as k8s
import sys
from util import *

ec2_client = boto3.client('ec2')

def create_cluster(mem_count, ebs_count, route_count, bench_count, cfile,
        ssh_key, cluster_name, kops_bucket, aws_key_id, aws_key):

    # create the cluster object with kops
    run_process(['./create_cluster_object.sh', cluster_name, kops_bucket,
        ssh_key])

    client = init_k8s()

    # create the kops pod
    print('Creating management pods...')
    kops_spec = load_yaml('yaml/pods/kops-pod.yml')
    env = kops_spec['spec']['containers'][0]['env']

    replace_yaml_val(env, 'AWS_ACCESS_KEY_ID', aws_key_id)
    replace_yaml_val(env, 'AWS_SECRET_ACCESS_KEY', aws_key)
    replace_yaml_val(env, 'KOPS_STATE_STORE', kops_bucket)
    replace_yaml_val(env, 'NAME', cluster_name)

    client.create_namespaced_pod(namespace=NAMESPACE, body=kops_spec)

    # wait for the kops pod to start
    kops_pod = client.list_namespaced_pod(namespace=NAMESPACE,
            label_selector='role=kops').items[0]
    while kops_pod.status.phase != 'Running':
        kops_pod = client.list_namespaced_pod(namespace=NAMESPACE,
                label_selector='role=kops').items[0]

    kops_ip = kops_pod.status.pod_ip

    # copy kube config file to kops pod, so it can execute kubectl commands
    kops_podname = kops_spec['metadata']['name']
    copy_file_to_pod(client, '/home/ubuntu/.kube/config', kops_podname,
            '/root/.kube/')
    copy_file_to_pod(client, ssh_key, kops_podname, '/root/.ssh/')
    copy_file_to_pod(client, ssh_key + '.pub', kops_podname,
            '/root/.ssh/')
    copy_file_to_pod(client, cfile, kops_podname, '/fluent/conf/')

    # start the monitoring pod
    mon_spec = load_yaml('yaml/pods/monitoring-pod.yml')
    replace_yaml_val(mon_spec['spec']['containers'][0]['env'], 'MGMT_IP',
            kops_ip)
    client.create_namespaced_pod(namespace=NAMESPACE, body=mon_spec)

    mon_ips = get_pod_ips(client, 'role=monitoring')

    print('Creating %d routing nodes...' % (route_count))
    add_nodes(client, cfile, ['routing'], [route_count], mon_ips)
    route_ips = get_pod_ips(client, 'role=routing')

    print('Creating %d memory, %d ebs, and %d benchmark node(s)...' %
            (mem_count, ebs_count, bench_count))
    add_nodes(client, cfile, ['memory', 'ebs', 'benchmark'],
            [mem_count, ebs_count, bench_count], mon_ips, route_ips)

    print('Finished creating all pods...')
    os.system('touch setup_complete')
    copy_file_to_pod(client, 'setup_complete', kops_podname, '/fluent')
    os.system('rm setup_complete')

    print('Creating routing service...')
    service_spec = load_yaml('yaml/services/routing.yml')
    client.create_namespaced_service(namespace=NAMESPACE,
            body=service_spec)

    service_name = service_spec['metadata']['name']
    service = client.read_namespaced_service(namespace=NAMESPACE,
            name=service_name)

    while service.status.load_balancer.ingress == None or \
            service.status.load_balancer.ingress[0].hostname == None:
        service = client.read_namespaced_service(namespace=NAMESPACE,
                name=service_name)

    sg_name = 'nodes.' + cluster_name
    sg = ec2_client.describe_security_groups(Filters=[{'Name': 'group-name',
        'Values': [sg_name]}])['SecurityGroups'][0]

    permissions = []
    for i in range(4):
        port = 6200 + i
        permission = {
                'FromPort': port,
                'IpProtocol': 'tcp',
                'ToPort': port,
                'IpRanges': [{
                    'CidrIp': '0.0.0.0/0'
                }]
        }
        permissions.append(permission)

    print('Authorizing ports for routing service...')
    ec2_client.authorize_security_group_ingress(GroupId=sg['GroupId'],
            IpPermissions=permissions)

    print('The service can be accessed via the following address: \n\t%s' %
            (service.status.load_balancer.ingress[0].hostname))



def parse_args(args, length, typ):
    result = []

    for arg in args[:length]:
        try:
            result.append(typ(arg))
        except:
            print('Unrecognized command-line argument %s. Could not convert \
                    to integer.' % (arg))
            sys.exit(1)

    return tuple(result)

if __name__ == '__main__':
    if len(sys.argv) < 4:
        print('Usage: ./create_cluster.py min_mem_instances min_ebs_instances \
                routing_instance benchmark_instances <path-to-conf-file> \
                <path-to-ssh-key>')
        print()
        print('If no SSH key is specified, we will use the default SSH key \
                (/home/ubuntu/.ssh/id_rsa). The corresponding public key is \
                assumed to have the same path and end in .pub.')
        print()
        print('If no config file is specific, the default base config file in \
                $FLUENT_HOME/conf/kvs-base.yml will be used.')
        sys.exit(1)

    mem, ebs, route, bench = parse_args(sys.argv[1:], 4, int)

    cluster_name = check_or_get_env_arg('NAME')
    kops_bucket = check_or_get_env_arg('KOPS_STATE_STORE')
    aws_key_id = check_or_get_env_arg('AWS_ACCESS_KEY_ID')
    aws_key = check_or_get_env_arg('AWS_SECRET_ACCESS_KEY')

    if len(sys.argv) <= 5:
        conf_file = '../conf/kvs-base.yml'
    else:
        conf_file = sys.argv[5]

    if len(sys.argv) <= 6:
        ssh_key = '/home/ubuntu/.ssh/id_rsa'
    else:
        ssh_key = sys.argv[6]

    create_cluster(mem, ebs, route, bench, conf_file, ssh_key, cluster_name,
            kops_bucket, aws_key_id, aws_key)
