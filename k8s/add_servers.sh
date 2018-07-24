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

if [ -z "$1" ] && [ -z "$2" ]; then
  echo "Usage: ./add_servers.sh <node-type> <new-instances> {<num-prev-instances>}"
  echo "Valid node types are memory, ebs, benchmark, and routing."
  echo "If number of previous instances is not specified, it is assumed to be 0."
  exit 1
fi

if [ -z "$3" ]; then
  $3=0
fi

if [ "$1" = "memory" ]; then
  YML_FILE=yaml/igs/memory-ig.yml
elif [ "$1" = "ebs" ]; then
  YML_FILE=yaml/igs/ebs-ig.yml
elif [ "$1" = "routing" ]; then
  YML_FILE=yaml/igs/routing-ig.yml
elif [ "$1" = "benchmark" ]; then
  YML_FILE=yaml/igs/benchmark-ig.yml
else
  echo "Unrecognized node type $1. Valid node types are memory, EBS, benchmark, and routing."
fi

NUM_INSTANCES=$(($2 + $3))

sed "s|CLUSTER_NAME|$NAME|g" $YML_FILE > tmp.yml
sed -i "s|NUM_DUMMY|$NUM_INSTANCES|g" tmp.yml

kops replace -f tmp.yml --force > /dev/null 2>&1
rm tmp.yml
