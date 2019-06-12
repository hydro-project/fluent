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
import os

import boto3
import kubernetes as k8s
from util import *

ec2_client = boto3.client('ec2', os.getenv('AWS_REGION', 'us-east-1'))


def remove_node(ip, ntype):
    client = init_k8s()

    pod = get_pod_from_ip(client, ip)
    hostname = 'ip-%s.ec2.internal' % (ip.replace('.', '-'))

    node = client.list_node(label_selector='kubernetes.io/hostname=' +
                                           hostname).items[0]

    unique_id = node.metadata.labels['podid'].split('-')[-1]
    podname = '%s-pod-%s' % (ntype, unique_id)
    client.delete_namespaced_pod(name=podname, namespace=NAMESPACE,
                                 body=k8s.client.V1DeleteOptions())
    client.delete_node(name=hostname, body=k8s.client.V1DeleteOptions())

    prev_count = get_previous_count(client, ntype)
    run_process(['./modify_ig.sh', ntype, str(prev_count - 1)])

    if ntype == 'ebs':
        vol_ids = list(map(lambda vol: vol.volume_id,
                           filter(lambda vol: vol is not None,
                                  map(lambda vol: vol.aws_elastic_block_store,
                                      pod.spec.volumes))))

        for vid in vol_ids:
            vol = ec2_client.describe_volumes(VolumeIds=[vid])['Volumes'][0]

            # wait for volume to no longer be in use
            while vol['State'] == 'in-use':
                vol = ec2_client.describe_volumes(
                      VolumeIds=[vid])['Volumes'][0]

            ec2_client.delete_volume(VolumeId=vid)
