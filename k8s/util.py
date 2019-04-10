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

import kubernetes as k8s
from kubernetes.stream import stream
import os
import subprocess
import sys
import tarfile
from tempfile import TemporaryFile
import yaml

NAMESPACE = 'default'
EBS_VOL_COUNT = 4

EXECUTOR_DEPART_PORT = 4050
EXECUTOR_PIN_PORT = 4000

def replace_yaml_val(yamlobj, name, val):
    for pair in yamlobj:
        if pair['name'] == name:
            pair['value'] = val
            return

def init_k8s():
    cfg = k8s.config
    cfg.load_kube_config()

    client = k8s.client.CoreV1Api()

    return client

def load_yaml(filename):
    try:
        with open(filename, 'r') as f:
            return yaml.load(f.read())
    except Error as e:
        print('Unexpected error while loading YAML file:')
        print(e.stderr)
        print('')
        print('Make sure to clean up the cluster object and state store \
                before recreating the cluster.')
        sys.exit(1)

def run_process(command):
    try:
        subprocess.run(command, cwd='./kops', check=True)
    except subprocess.CalledProcessError as e:
        print('Unexpected error while running command %s:' % (e.cmd))
        print(e.stderr)
        print('')
        print('Make sure to clean up the cluster object and state store ' +
                'before recreating the cluster.')
        sys.exit(1)

def check_or_get_env_arg(argname):
    if argname not in os.environ:
        print('Required argument %s not found as an environment variable.' +
                'Please specify before re-running.' % (argname))
        sys.exit(1)

    return os.environ[argname]

def get_pod_ips(client, selector, isRunning=False):
    pod_list = client.list_namespaced_pod(namespace=NAMESPACE,
            label_selector=selector).items

    pod_ips = list(map(lambda pod: pod.status.pod_ip, pod_list))

    running = False
    while None in pod_ips or not running:
        pod_list = client.list_namespaced_pod(namespace=NAMESPACE,
                label_selector=selector).items
        pod_ips = list(map(lambda pod: pod.status.pod_ip, pod_list))

        if isRunning:
            pod_statuses = list(filter(lambda pod: pod.status.phase !=
                'Running', pod_list))
            running = len(pod_statuses) == 0
        else:
            running = True

    return pod_ips

def _get_executor_depart_address(ip, tid):
    return 'tcp://' + ip + ':' + str(tid + EXECUTOR_DEPART_PORT)

def _get_executor_pin_address(ip, tid):
    return 'tcp://' + ip + ':' + str(tid + EXECUTOR_PIN_PORT)

def get_previous_count(client, kind):
    selector = 'role=%s' % (kind)
    items = client.list_namespaced_pod(namespace=NAMESPACE,
            label_selector=selector).items

    return len(items)

def get_pod_from_ip(client, ip):
    pods = client.list_namespaced_pod(namespace=NAMESPACE).items
    pod = list(filter(lambda pod: pod.status.pod_ip == ip, pods))[0]

    return pod

def get_service_address(client, svc_name):
    service = client.read_namespaced_service(namespace=NAMESPACE,
            name=svc_name)

    while service.status.load_balancer.ingress == None or \
            service.status.load_balancer.ingress[0].hostname == None:
        service = client.read_namespaced_service(namespace=NAMESPACE,
                name=svc_name)

    return service.status.load_balancer.ingress[0].hostname

# from https://github.com/aogier/k8s-client-python/blob/12f1443895e80ee24d689c419b5642de96c58cc8/examples/exec.py#L101
def copy_file_to_pod(client, filepath, podname, podpath, container):
    exec_command = ['tar', 'xmvf', '-', '-C', podpath]
    resp = stream(client.connect_get_namespaced_pod_exec, podname, NAMESPACE,
                  command=exec_command,
                  stderr=True, stdin=True,
                  stdout=True, tty=False,
                  _preload_content=False, container=container)

    filename = filepath.split('/')[-1]
    with TemporaryFile() as tar_buffer:
        with tarfile.open(fileobj=tar_buffer, mode='w') as tar:
            tar.add(filepath, arcname=filename)

        tar_buffer.seek(0)
        commands = []
        commands.append(str(tar_buffer.read(), 'utf-8'))

        while resp.is_open():
            resp.update(timeout=1)
            if resp.peek_stdout():
                pass
            if resp.peek_stderr():
                print("Unexpected error while copying files: %s" %
                        (resp.read_stderr()))
                sys.exit(1)
            if commands:
                c = commands.pop(0)
                resp.write_stdin(c)
            else:
                break
        resp.close()

