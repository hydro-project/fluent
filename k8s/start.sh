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

if [ -z "$1" ]; then
  echo "No argument provided. Exiting."
  exit 1
fi

gen_yml_list() {
  IFS=' ' read -r -a ARR <<< $1
  RESULT=""

  for IP in "${ARR[@]}"; do
    RESULT=$"$RESULT        - $IP\n"
  done

  echo -e "$RESULT"
}

cd fluent
mkdir -p conf

IS_EC2=`curl -s http://instance-data.ec2.internal`
PRIVATE_IP=`ifconfig eth0 | grep 'inet addr:' | grep -v '127.0.0.1' | cut -d: -f2 | awk '{ print $1 }'`
if [[ ! -z "$IS_EC2" ]]; then
  PUBLIC_IP=`curl http://169.254.169.254/latest/meta-data/public-ipv4`
else
  PUBLIC_IP=$PRIVATE_IP
fi



echo -e "threads:" > conf/kvs-config.yml
echo -e "    memory: 4" >> conf/kvs-config.yml
echo -e "    ebs: 4" >> conf/kvs-config.yml
echo -e "    shared-memory: 4" >> conf/kvs-config.yml
echo -e "    benchmark: 4" >> conf/kvs-config.yml
echo -e "    routing: 4" >> conf/kvs-config.yml

echo -e "replication:" >> conf/kvs-config.yml
echo -e "    memory: 1" >> conf/kvs-config.yml
echo -e "    ebs: 0" >> conf/kvs-config.yml
echo -e "    shared-memory: 0" >> conf/kvs-config.yml
echo -e "    minimum: 1" >> conf/kvs-config.yml
echo -e "    local: 1" >> conf/kvs-config.yml

if [ "$1" = "mn" ]; then
  echo -e "monitoring:" >> conf/kvs-config.yml
  echo -e "    mgmt_ip: $MGMT_IP" >> conf/kvs-config.yml
  echo -e "    ip: $PRIVATE_IP" >> conf/kvs-config.yml

  ./build/kvs/src/monitor/flmonitor
elif [ "$1" = "r" ]; then
  echo -e "routing:" >> conf/kvs-config.yml
  echo -e "    ip: $PRIVATE_IP" >> conf/kvs-config.yml

  LST=$(gen_yml_list "$MON_IPS")
  echo -e "    monitoring:" >> conf/kvs-config.yml
  echo -e "$LST" >> conf/kvs-config.yml

  ./build/kvs/src/route/flroute
elif [ "$1" = "b" ]; then
  echo -e "user:" >> conf/kvs-config.yml
  echo -e "    ip: $PRIVATE_IP" >> conf conf/kvs-config.yml

  LST=$(gen_yml_list "$MON_IPS")
  echo -e "    monitoring:" >> conf/kvs-config.yml
  echo -e "$LST" >> conf/kvs-config.yml

  LST=$(gen_yml_list "$ROUTING_IPS")
  echo -e "    routing:" >> conf/kvs-config.yml
  echo -e "$LST" >> conf/kvs-config.yml

  ./build/kvs/src/benchmark/flbench
else
  echo -e "server:" >> conf/kvs-config.yml
  echo -e "    seed_ip: $SEED_IP" >> conf/kvs-config.yml
  echo -e "    public_ip: $PUBLIC_IP" >> conf/kvs-config.yml
  echo -e "    private_ip: $PRIVATE_IP" >> conf/kvs-config.yml
  echo -e "    mgmt_ip: $MGMT_IP" >> conf/kvs-config.yml

  LST=$(gen_yml_list "$MON_IPS")
  echo -e "    monitoring:" >> conf/kvs-config.yml
  echo -e "$LST" >> conf/kvs-config.yml

  LST=$(gen_yml_list "$ROUTING_IPS")
  echo -e "    routing:" >> conf/kvs-config.yml
  echo -e "$LST" >> conf/kvs-config.yml

  ./build/kvs/src/kvs/flkvs
fi
