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

while [[ ! -f "conf/kvs-config.yml" ]]; do
  continue
done

echo -e "user:" >> conf/kvs-config.yml
echo -e "    ip: $PRIVATE_IP" >> conf/kvs-config.yml
echo -e "    routing-elb: $ROUTE_ADDR" >> conf/kvs-config.yml

./build/functions/cache/src/flfunc-cache
