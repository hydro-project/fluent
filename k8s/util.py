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
import os
import subprocess
import sys
import yaml

NAMESPACE = 'default'
EBS_VOL_COUNT = 4

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
        print('Make sure to clean up the cluster object and state store \
                before recreating the cluster.')
        sys.exit(1)

def check_or_get_env_arg(argname):
    if argname not in os.environ:
        print('Required argument %s not found as an environment variable. \
                Please specify before re-running.' % (argname))
        sys.exit(1)

    return os.environ[argname]

def get_pod_ips(client, selector):
    pod_list = client.list_namespaced_pod(namespace=NAMESPACE,
            label_selector=selector).items

    pod_ips = list(map(lambda pod: pod.status.pod_ip, pod_list))

    while None in pod_ips:
        pod_list = client.list_namespaced_pod(namespace=NAMESPACE,
                label_selector=selector).items
        pod_ips = list(map(lambda pod: pod.status.pod_ip, pod_list))

    return pod_ips

def get_previous_count(client, kind):
    selector = 'role=%s' % (kind)
    items = client.list_namespaced_pod(namespace=NAMESPACE,
            label_selector=selector).items

    return len(items)

def get_pod_from_ip(client, ip):
    pods = client.list_namespaced_pod(namespace=NAMESPACE).items
    pod = list(filter(lambda pod: pod.status.pod_ip == ip, pods))[0]

    return pod
