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

import boto3
import argparse
import os
import sys

from add_nodes import add_nodes
import util

ec2_client = boto3.client('ec2', os.getenv('AWS_REGION', 'us-east-1'))


def create_cluster(mem_count, ebs_count, func_count, sched_count, route_count,
                   bench_count, cfile, ssh_key, cluster_name, kops_bucket,
                   aws_key_id, aws_key):

    # create the cluster object with kops
    util.run_process(['./create_cluster_object.sh', cluster_name, kops_bucket,
                      ssh_key])

    client, apps_client = util.init_k8s()

    # create the kops pod
    print('Creating management pods...')
    kops_spec = util.load_yaml('yaml/pods/kops-pod.yml')
    env = kops_spec['spec']['containers'][0]['env']

    util.replace_yaml_val(env, 'AWS_ACCESS_KEY_ID', aws_key_id)
    util.replace_yaml_val(env, 'AWS_SECRET_ACCESS_KEY', aws_key)
    util.replace_yaml_val(env, 'KOPS_STATE_STORE', kops_bucket)
    util.replace_yaml_val(env, 'FLUENT_CLUSTER_NAME', cluster_name)

    client.create_namespaced_pod(namespace=util.NAMESPACE, body=kops_spec)

    # wait for the kops pod to start
    kops_ip = util.get_pod_ips(client, 'role=kops', is_running=True)[0]

    # copy kube config file to kops pod, so it can execute kubectl commands
    kops_podname = kops_spec['metadata']['name']
    kcname = kops_spec['spec']['containers'][0]['name']

    os.system('cp %s kvs-config.yml' % cfile)
    util.copy_file_to_pod(client, '/home/ubuntu/.kube/config', kops_podname,
                          '/root/.kube/', kcname)
    util.copy_file_to_pod(client, ssh_key, kops_podname, '/root/.ssh/', kcname)
    util.copy_file_to_pod(client, ssh_key + '.pub', kops_podname,
                          '/root/.ssh/', kcname)
    util.copy_file_to_pod(client, 'kvs-config.yml', kops_podname,
                          '/fluent/conf/', kcname)

    # start the monitoring pod
    mon_spec = util.load_yaml('yaml/pods/monitoring-pod.yml')
    util.replace_yaml_val(mon_spec['spec']['containers'][0]['env'], 'MGMT_IP',
                          kops_ip)
    client.create_namespaced_pod(namespace=util.NAMESPACE, body=mon_spec)

    util.get_pod_ips(client, 'role=monitoring')

    # copy config file into monitoring pod -- wait till we create routing pods,
    # so we're sure that the monitoring nodes are up and running
    util.copy_file_to_pod(client, 'kvs-config.yml',
                          mon_spec['metadata']['name'],
                          '/fluent/conf/',
                          mon_spec['spec']['containers'][0]['name'])
    os.system('rm kvs-config.yml')

    print('Creating %d routing nodes...' % (route_count))
    add_nodes(client, apps_client, cfile, ['routing'], [route_count], True)
    util.get_pod_ips(client, 'role=routing')

    print('Creating %d memory, %d ebs node(s)...' %
          (mem_count, ebs_count))
    add_nodes(client, apps_client, cfile, ['memory', 'ebs'],
              [mem_count, ebs_count], True)

    print('Creating routing service...')
    service_spec = util.load_yaml('yaml/services/routing.yml')
    client.create_namespaced_service(namespace=util.NAMESPACE,
                                     body=service_spec)

    print('Adding %d scheduler nodes...' % (sched_count))
    add_nodes(client, apps_client, cfile, ['scheduler'], [sched_count], True)
    util.get_pod_ips(client, 'role=scheduler')

    print('Adding %d function serving nodes...' % (func_count))
    add_nodes(client, apps_client, cfile, ['function'], [func_count], True)

    print('Creating function service...')
    service_spec = util.load_yaml('yaml/services/function.yml')
    client.create_namespaced_service(namespace=util.NAMESPACE,
                                     body=service_spec)

    print('Adding %d benchmark nodes...' % (bench_count))
    add_nodes(client, apps_client, cfile, ['benchmark'], [bench_count], True)

    print('Finished creating all pods...')
    os.system('touch setup_complete')
    util.copy_file_to_pod(client, 'setup_complete', kops_podname, '/fluent',
                          kcname)
    os.system('rm setup_complete')

    sg_name = 'nodes.' + cluster_name
    sg = ec2_client.describe_security_groups(
          Filters=[{'Name': 'group-name',
                    'Values': [sg_name]}])['SecurityGroups'][0]

    print('Authorizing ports for routing service...')

    permission = [{
        'FromPort': 6200,
        'IpProtocol': 'tcp',
        'ToPort': 6203,
        'IpRanges': [{
            'CidrIp': '0.0.0.0/0'
        }]
    }]
    ec2_client.authorize_security_group_ingress(GroupId=sg['GroupId'],
                                                IpPermissions=permission)

    routing_svc_addr = util.get_service_address(client, 'routing-service')
    function_svc_addr = util.get_service_address(client, 'function-service')
    print('The routing service can be accessed here: \n\t%s' %
          (routing_svc_addr))
    print('The function service can be accessed here: \n\t%s' %
          (function_svc_addr))


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='''Creates a Hydro cluster
                                     using Kubernetes and kops. If no SSH key
                                     is specified, we use the default SSH key
                                     (~/.ssh/id_rsa), and we expect that the
                                     correponding public key has the same path
                                     and ends in .pub.

                                     If no configuration file base is
                                     specified, we use the default
                                     ($FLUENT_HOME/conf/kvs-base.yml).''')

    parser.add_argument('-m', '--memory', nargs=1, type=int, metavar='M',
                        help='The number of memory nodes to start with ' +
                        '(required)', dest='memory', required=True)
    parser.add_argument('-r', '--routing', nargs=1, type=int, metavar='R',
                        help='The number of routing  nodes in the cluster ' +
                        '(required)', dest='routing', required=True)
    parser.add_argument('-f', '--function', nargs=1, type=int, metavar='F',
                        help='The number of function nodes to start with ' +
                        '(required)', dest='function', required=True)
    parser.add_argument('-s', '--scheduler', nargs=1, type=int, metavar='S',
                        help='The number of scheduler nodes to start with ' +
                        '(required)', dest='scheduler', required=True)
    parser.add_argument('-e', '--ebs', nargs='?', type=int, metavar='E',
                        help='The number of EBS nodes to start with ' +
                        '(optional)', dest='ebs', default=0)
    parser.add_argument('-b', '--benchmark', nargs='?', type=int, metavar='B',
                        help='The number of benchmark nodes in the cluster ' +
                        '(optional)', dest='benchmark', default=0)
    parser.add_argument('--conf', nargs='?', type=str,
                        help='The configuration file to start the cluster with'
                        + ' (optional)', dest='conf',
                        default='../conf/kvs-base.yml')
    parser.add_argument('--ssh-key', nargs='?', type=str,
                        help='The SSH key used to configure and connect to ' +
                        'each node (optional)', dest='sshkey',
                        default='~/.ssh/id_rsa')

    cluster_name = util.check_or_get_env_arg('FLUENT_CLUSTER_NAME')
    kops_bucket = util.check_or_get_env_arg('KOPS_STATE_STORE')
    aws_key_id = util.check_or_get_env_arg('AWS_ACCESS_KEY_ID')
    aws_key = util.check_or_get_env_arg('AWS_SECRET_ACCESS_KEY')

    args = parser.parse_args()

    create_cluster(args.memory, args.ebs, args.function, args.scheduler,
                   args.routing, args.benchmark, args.conf, args.sshkey,
                   cluster_name, kops_bucket, aws_key_id, aws_key)
