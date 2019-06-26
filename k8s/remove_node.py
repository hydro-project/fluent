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
import os

import boto3
import kubernetes as k8s
import util

ec2_client = boto3.client('ec2', os.getenv('AWS_REGION', 'us-east-1'))


def remove_node(ip, ntype):
    client, _ = util.init_k8s()

    pod = util.get_pod_from_ip(client, ip)
    hostname = 'ip-%s.ec2.internal' % (ip.replace('.', '-'))

    podname = pod.metadata.name
    client.delete_namespaced_pod(name=podname, namespace=util.NAMESPACE,
                                 body=k8s.client.V1DeleteOptions())
    client.delete_node(name=hostname, body=k8s.client.V1DeleteOptions())

    prev_count = util.get_previous_count(client, ntype)
    util.run_process(['./modify_ig.sh', ntype, str(prev_count - 1)])
