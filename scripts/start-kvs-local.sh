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

if [ -z "$1" ] && [ -z "$2" ]; then
  echo "Usage: ./scripts/start_local.sh build start-user"
  echo ""
  echo "You must run this from the project root directory."
  exit 1
fi

if [ "$1" = "y" ] || [ "$1" = "yes" ]; then
  ./scripts/build.sh
fi

cp conf/kvs-example.yml conf/kvs-config.yml

./build/kvs/src/monitor/flmonitor &
MPID=$!
./build/kvs/src/route/flroute &
RPID=$!
export SERVER_TYPE="memory"
./build/kvs/src/kvs/flkvs &
SPID=$!

echo $MPID > pids
echo $RPID >> pids
echo $SPID >> pids

if [ "$2" = "y" ] || [ "$2" = "yes" ]; then
  ./build/kvs/client/cpp/flkvs-cli conf/kvs-config.yml
fi
