#!/bin/bash

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

if [[ -z "$1" ]] || [[ -z "$2" ]]; then
  echo "Usage ./remove_node.sh <node-id> <node-type>"
  exit 1
fi

get_prev_num() {
  NUM_PREV=`kubectl get pods -l role=$1 | wc -l`

  if [ $NUM_PREV -gt 0 ]; then
    ((NUM_PREV--))
  fi

  echo $NUM_PREV
}

echo $2

# convert from IP to a hostname
EXTERNAL_IP=`kubectl get pods -o json | jq '.items[] | select(.status.podIP=="'$1'")' | jq '.status.hostIP' | cut -d'"' -f2`
IP=`echo $EXTERNAL_IP | sed "s|\.|-|g"`
HN=ip-$IP.ec2.internal
echo $EXTERNAL_IP
echo $IP
echo $HN

# retrieve by host name and get the unique id
LABEL=`kubectl get node -l kubernetes.io/hostname=$HN -o jsonpath='{.items[*].metadata.labels.podid}' | cut -d'-' -f2`

if [ "$2" = "memory" ]; then
  YML_FILE="yaml/igs/memory-ig.yml"
elif [ "$2" = "ebs" ]; then
  YML_FILE="yaml/igs/ebs-ig.yml"
  EBS_VOLS=`kubectl get pod ebs-pod-$LABEL -o jsonpath='{.spec.volumes[*].awsElasticBlockStore.volumeID}'`
else
  echo "Unrecognized node type: $1."
  exit 1
fi

# delete the pod and instance groups
NUM_PREV=$(get_prev_num $2)
((NUM_PREV--))
sed "s|NUM_DUMMY|$NUM_PREV|g" $YML_FILE > tmp.yml
sed -i "s|CLUSTER_NAME|$NAME|g"  tmp.yml
echo $YML_FILE
echo $NUM_PREV

kubectl delete pod memory-pod-$LABEL
kubectl delete node $HN
kops replace -f tml.yml --force >> /dev/null 2>&1
rm tmp.yml

# if we're dropping an ebs instance, delete the volume
if [ "$1" = "ebs" ]; then
  for vol in $EBS_VOLS; do
    aws ec2 delete-volume --volume-id $vol
  done
fi
