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

if [[ -z "$1" ]] && [[ -z "$2" ]] && [[ -z "$3" ]] && [[ -z "$4" ]]; then
  echo "Usage: ./add_node.sh <memory-nodes> <ebs-nodes> <routing-nodes> <benchmark-nodes>"
  echo ""
  echo "Expected usage is calling add_nodes, which in turn adds servers (using add_servers.sh)."
  exit 1
fi

# get the ips of all the different kinds of nodes in the system
ROUTING_IPS=`kubectl get pods -l role=routing -o jsonpath='{.items[*].status.podIP}'`

# this one should never be empty
MON_IPS=`kubectl get pods -l role=monitoring -o jsonpath='{.items[*].status.podIP}' | tr -d '[:space:]'`
while [ "$MON_IPS" = "" ]; do
  MON_IPS=`kubectl get pods -l role=monitoring -o jsonpath='{.items[*].status.podIP}' | tr -d '[:space:]'`
done

get_prev_num() {
  NUM_PREV=`kubectl get pods -l role=$1 | wc -l`

  if [ $NUM_PREV -gt 0 ]; then
    ((NUM_PREV--))
  fi

  echo $NUM_PREV
}

add_servers() {
  if [ $2 -gt 0 ]; then
    NUM_PREV=$(get_prev_num $1)

    ./add_servers.sh $1 $2 $NUM_PREV
  fi
}

add_servers memory $1
add_servers ebs $2
add_servers routing $3
add_servers benchmark $4

kops update cluster ${NAME} --yes > /dev/null 2>&1

kops validate cluster > /dev/null 2>&1
while [ $? -ne 0 ]; do
  kops validate cluster > /dev/null 2>&1
done

add_pods() {
  NUM_PREV=$(get_prev_num $1)

  NAMES=`kubectl get nodes -l role=$1 --sort-by=.metadata.creationTimestamp | tail -n $2 | cut -d' ' -f1` > /dev/null 2>&1
  IFS=' ' read -r -a IPS <<< $NAMES

  for i in "${!IPS[@]}"; do
    ID=$(($i + 1 + $NUM_PREV))

    kubectl label nodes ${IPS[i]} podid=$1-$ID > /dev/null 2>&1
  done

  if [ "$1" = "memory" ]; then
    YML_FILE=yaml/pods/memory-pod.yml
  elif [ "$1" = "benchmark" ]; then
    YML_FILE=yaml/pods/benchmark-pod.yml
  elif [ "$1" = "ebs" ]; then
    YML_FILE=yaml/pods/ebs-pod.yml
  elif [ "$1" = "routing" ]; then
    YML_FILE=yaml/pods/routing-pod.yml
  else
    exit 1
  fi

  # split the proxies into an array and choose a random one as the seed
  IFS=' ' read -ra ARR <<< "$ROUTING_IPS"
  if [ ${#ARR[@]} -eq 0 ]; then
    SEED_IP=""
  else
    SEED_IP=${ARR[$RANDOM % ${#ARR[@]}]}
  fi

  FINAL_NUM=$(($NUM_PREV + $2))
  ((NUM_PREV++))

  for i in $(seq $NUM_PREV $FINAL_NUM); do
    sed "s|NUM_DUMMY|$i|g" $YML_FILE > tmp.yml

    if [ "$1" = "ebs" ]; then
      # create new EBS volume
      EBS_V0=`aws ec2 create-volume --availability-zone=us-east-1a --size=64 --volume-type=gp2 | grep VolumeId | cut -d\" -f4`
      aws ec2 create-tags --resources $EBS_V0 --tags Key=KubernetesCluster,Value=$NAME > /dev/null 2>&1
      EBS_V1=`aws ec2 create-volume --availability-zone=us-east-1a --size=64 --volume-type=gp2 | grep VolumeId | cut -d\" -f4`
      aws ec2 create-tags --resources $EBS_V1 --tags Key=KubernetesCluster,Value=$NAME > /dev/null 2>&1
      EBS_V2=`aws ec2 create-volume --availability-zone=us-east-1a --size=64 --volume-type=gp2 | grep VolumeId | cut -d\" -f4`
      aws ec2 create-tags --resources $EBS_V2 --tags Key=KubernetesCluster,Value=$NAME > /dev/null 2>&1
      EBS_V3=`aws ec2 create-volume --availability-zone=us-east-1a --size=64 --volume-type=gp2 | grep VolumeId | cut -d\" -f4`
      aws ec2 create-tags --resources $EBS_V3 --tags Key=KubernetesCluster,Value=$NAME > /dev/null 2>&1

      # set EBS volume IDs
      sed -i "s|VOLUME_DUMMY_0|$EBS_V0|g" tmp.yml
      sed -i "s|VOLUME_DUMMY_1|$EBS_V1|g" tmp.yml
      sed -i "s|VOLUME_DUMMY_2|$EBS_V2|g" tmp.yml
      sed -i "s|VOLUME_DUMMY_3|$EBS_V3|g" tmp.yml
    fi

    # set the IPs of other system components
    sed -i "s|ROUTING_IPS_DUMMY|\"$ROUTING_IPS\"|g" tmp.yml
    sed -i "s|MON_IPS_DUMMY|$MON_IPS|g" tmp.yml
    sed -i "s|SEED_IP_DUMMY|$SEED_IP|g" tmp.yml

    kubectl create -f tmp.yml > /dev/null 2>&1
    rm tmp.yml
  done
}

add_pods memory $1
add_pods ebs $2
add_pods routing $3
add_pods benchmark $4

