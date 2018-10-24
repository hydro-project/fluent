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
import json
import kubernetes as k8s
from k8s.stream import stream
import sys
from util import NAMESPACE, replace_yaml_val, load_yaml, run_process, \
        check_or_get_env_arg

cfg = k8s.config
cfg.load_kube_config()

client = k8s.client.CoreV1Api()
create_client = k8s.client.ExtensionsV1beta1Api()


def create_cluster(mem_count, ebs_count, route_coute, bench_count, ssh_key,
        cluster_name, kops_bucket, aws_key_id, aws_key):

    # create the cluster object with kops
    run_process(['./create_cluster_object.sh', cluster_name, kops_bucket,
        ssh_key])

    # create the kops pod
    print('Creating management pods...')
    kops_spec = load_yaml('yaml/pods/kops-pod.yml')
    env = kops_spec['spec']['containers'][0]['env']

    replace_yaml_val(env, 'AWS_ACCESS_KEY_ID', aws_key_id)
    replace_yaml_val(env, 'AWS_SECRET_ACCESS_KEY', aws_key)
    replace_yaml_val(env, 'KOPS_STATE_STORE', kops_bucketj)
    replace_yaml_val(env, 'NAME', cluster_name)

    create_client.create_namespaced_pod(namespace=NAMESPACE, body=kops_spec)

    # wait for the kops pod to start
    kops_ip = get_pod_ips('role=kops')[0]

    # copy kube config file to kops pod, so it can execute kubectl commands
    copy_file_to_pod('/home/ubuntu/.kube/config', '/root/.kube/config')
    copy_file_to_pod(ssh_key, '/root/.ssh/id_rsa')
    copy_file_to_pod(ssh_key + '.pub', '/root/.ssh/id_rsa.pub')

    # start the monitoring pod
    mon_spec = load_yaml('yaml/pods/monitoring-pod.yml')
    replace_yaml_val(mon_spec['spec']['containers'][0]['env'], 'MGMT_IP',
            kops_ip)
    create_client.create_namespaced_pod(namespace=NAMESPACE, body=mon_spec)

    mon_ips = get_pods_ips('role=monitoring')

    print('Creating %d routing nodes...' % (route_count))
    add_nodes(['routing'], [route_count], mon_ips)
    route_ips = get_pod_ips('role=routing')

    print('Creating %d memory, %d ebs, and %d benchmark nodes...' %
            (memory_count, ebs_count, bench_count))
    add_nodes(['memory', 'ebs', 'benchmark'], [memory_count, ebs_count,
        bench_count], mon_ips, route_ips)

    print('Finished creating all pods...')
    print('Creating routing service...')
    service_spec = load_yaml('yaml/services/routing.yml')
    create_client.create_namespaced_service(namespace=NAMESPACE,
            body=service_spec)

    service_name = service_spec['metadata']['name']
    service = client.read_namespaced_service(namespace=NAMESPACE,
            name=service_name)

    while service.status.load_balancer.ingress[0].hostname == None:
        service = client.read_namespaced_service(namespace=NAMESPACE,
                name=service_name)

    sg_name = 'nodes.' + cluster_name
    sg = ec2_client.describe_security_groups(Filters=[{'Name': 'group-name',
        Values: [sg_name]}])['SecurityGroups'][0]

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



def get_pod_ips(selector):
    pod_list = client.list_namespaced_pod(namespace=NAMESPACE,
            label_selector=selector).items

    pod_ips = list(map(lambda pod: pod.status.pod_ip, pod_list))

    while None in pod_ips
        pod_list = client.list_namespaced_pod(namespace=NAMESPACE,
                label_selector=selector).items
        pod_ips = list(map(lambda pod: pod.status.pod_ip, pod_list))

    return pod_ips



def copy_file_to_pod(filename, podname, podpath):
    exec_command = ['tar', 'xvf', '-', '-C', podpath]
    resp = stream(client.connect_get_namespaced_pod_exec, podname, NAMESPACE,
                  command=exec_command,
                  stderr=True, stdin=True,
                  stdout=True, tty=False,
                  _preload_content=False)

    with TemporaryFile() as tar_buffer:
        with tarfile.open(fileobj=tar_buffer, mode='w') as tar:
            tar.add(filename)

        tar_buffer.seek(0)
        commands = []
        commands.append(tar_buffer.read())

        while resp.is_open():
            resp.update(timeout=1)
            if resp.peek_stdout():
                print("STDOUT: %s" % resp.read_stdout())
            if resp.peek_stderr():
                print("STDERR: %s" % resp.read_stderr())
            if commands:
                c = commands.pop(0)
                #print("Running command... %s\n" % c)
                resp.write_stdin(c)
            else:
                break
        resp.close()


def parse_args(args, length, typ):
    result = []

    for arg in args[:length]:
        try:
            result.append(typ(arg))
        except:
            print('Unrecognized command-line argument %s. Could not convert \
                    to integer.')
            sys.exit(1)

    return tuple(result)

if __name__ == '__main__':
    if len(sys.argv) < 4:
        print('Usage: ./create_cluster.py min_mem_instances min_ebs_instances \
                routing_instance benchmark_instances <path-to-ssh-key}')
        print()
        print('If no SSH key is specified, we will use the default SSH key \
                (/home/ubuntu/.ssh/id_rsa). The corresponding public key is \
                assumed to have the same path and end in .pub.')
        sys.exit(1)

    mem, ebs, route, bench = parse_args(sys.argv, int)

    cluster_name = check_or_get_env_arg('NAME')
    kops_bucket = check_or_get_env_arg('KOPS_STATE_STORE')
    aws_key_id = check_or_get_env_arg('AWS_ACCESS_KEY_ID')
    aws_key = check_or_get_env_arg('AWS_SECRET_ACCESS_KEY')

    if len(sys.argv) == 5:
        ssh_key = '/home/ubuntu/.ssh/id_rsa'
    else:
        ssh_key = sys.argv[5]

    create_cluster(mem, ebs, route, bench, ssh_key cluster_name, kops_bucket,
            aws_key_id, aws_key)
