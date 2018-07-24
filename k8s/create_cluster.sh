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

#!/bin/bash

if [ -z "$1" ] && [ -z "$2"] && [ -z "$3"] && [ -z "$4" ]; then
  echo "Usage: ./create_cluster.sh <min_mem_instances> <min_ebs_instances> <routing_instances> <benchmark_instances> {<path-to-ssh-key>}"
  echo ""
  echo "If no SSH key is specified, it is assumed that we are using the default SSH key (/home/ubuntu/.ssh/id_rsa). We assume that the corresponding public key has the same name and ends in .pub."
  exit 1
fi
if [ -z "$5" ]; then
  SSH_KEY=/home/ubuntu/.ssh/id_rsa
else
  SSH_KEY=$5
fi

export NAME=ucbrisebedrock.de
export KOPS_STATE_STORE=s3://tiered-storage-state-store

echo "Creating cluster object..."
kops create cluster --zones us-east-1a --ssh-public-key ${SSH_KEY}.pub ${NAME} --networking kopeio-vxlan > /dev/null 2>&1
# delete default instance group that we won't use
kops delete ig nodes --name ${NAME} --yes > /dev/null 2>&1

# add the kops node
echo "Adding general instance group"
sed "s|CLUSTER_NAME|$NAME|g" yaml/igs/general-ig.yml > tmp.yml
kops create -f tmp.yml > /dev/null 2>&1
rm tmp.yml

# create the cluster with just the routing instance group
echo "Creating cluster on AWS..."
kops update cluster --name ${NAME} --yes > /dev/null 2>&1

# wait until the cluster was created
echo "Validating cluster..."
kops validate cluster > /dev/null 2>&1
while [ $? -ne 0 ]
do
  kops validate cluster > /dev/null 2>&1
done

# create the kops pod
echo "Creating management pods"
sed "s|ACCESS_KEY_ID_DUMMY|$AWS_ACCESS_KEY_ID|g" yaml/pods/kops-pod.yml > tmp.yml
sed -i "s|SECRET_KEY_DUMMY|$AWS_SECRET_ACCESS_KEY|g" tmp.yml
sed -i "s|KOPS_BUCKET_DUMMY|$KOPS_STATE_STORE|g" tmp.yml
sed -i "s|CLUSTER_NAME|$NAME|g" tmp.yml
kubectl create -f tmp.yml > /dev/null 2>&1
rm tmp.yml

MGMT_IP=`kubectl get pods -l role=kops -o jsonpath='{.items[*].status.podIP}' | tr -d '[:space:]'`
while [ "$MGMT_IP" = "" ]; do
  MGMT_IP=`kubectl get pods -l role=kops -o jsonpath='{.items[*].status.podIP}' | tr -d '[:space:]'`
done

# copy kubecfg into the kops pod, so it can execute kops commands
kubectl cp /home/ubuntu/.kube/config kops-pod:/root/.kube/config > /dev/null 2>&1

sed "s|MGMT_IP_DUMMY|$MGMT_IP|g" yaml/pods/monitoring-pod.yml > tmp.yml
kubectl create -f tmp.yml > /dev/null 2>&1
rm tmp.yml

./add_nodes.sh 0 0 $3 0

# wait for all proxies to be ready
ROUTING_IPS=`kubectl get pods -l role=routing -o jsonpath='{.items[*].status.podIP}'`
ROUTING_IP_ARR=($ROUTING_IPS)
while [ ${#ROUTING_IP_ARR[@]} -ne $3 ]; do
  ROUTING_IPS=`kubectl get pods -l role=routing -o jsonpath='{.items[*].status.podIP}'`
  ROUTING_IP_ARR=($ROUTING_IPS)
done

./add_nodes.sh $1 $2 0 $4

# copy the SSH key into the management node... doing this later because we need
# to wait for the pod to come up
kubectl cp $SSH_KEY kops-pod:/root/.ssh/id_rsa > /dev/null 2>&1
kubectl cp ${SSH_KEY}.pub kops-pod:/root/.ssh/id_rsa.pub > /dev/null 2>&1

echo "Cluster is now ready for use!"
