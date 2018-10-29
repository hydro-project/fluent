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

if [[ -z "$1" ]] && [[ -z "$2" ]]; then
  echo "Usage: ./create_cluster_object.sh cluster-name cluster-state-store <path-to-ssh-key>"
  echo ""
  echo "Cluster name and S3 Bucket used as kops state store must be specified."
  echo "If no SSH key is specified, the default SSH key (~/.ssh/id_rsa) will be used."

  exit 1
fi

if [[ -z "$3" ]]; then
  if [[ ! -f "~/.ssh/id_rsa" ]]; then
    echo "No SSH key specified and default SSH key (~/.ssh/id_rsa) does not exist."

    exit 1
  fi

  SSH_KEY=~/.ssh/id_rsa
else
  SSH_KEY=$3
fi

if [[ -z "$AWS_ACCESS_KEY_ID" ]] || [[ -z "$AWS_SECRET_ACCESS_KEY" ]]; then
  echo "AWS access credentials are required to be stored in local environment variables for cluster creation."
  echo "Please use the AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY variables."

  exit 1
fi

NAME=$1
KOPS_STATE_STORE=$2

echo "Creating cluster object..."
kops create cluster --zones us-east-1a --ssh-public-key ${SSH_KEY}.pub ${NAME} --networking kube-router > /dev/null 2>&1
# delete default instance group that we won't use
kops delete ig nodes --name ${NAME} --yes > /dev/null 2>&1

echo "Adding general instance group"
sed "s|CLUSTER_NAME|$NAME|g" yaml/igs/general-ig.yml > tmp.yml
kops create -f tmp.yml > /dev/null 2>&1
rm tmp.yml

# create the cluster with just the routing instance group
echo "Creating cluster on AWS..."
kops update cluster --name ${NAME} --yes > /dev/null 2>&1

./validate_cluster.sh
