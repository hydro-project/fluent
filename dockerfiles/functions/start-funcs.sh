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

IS_EC2=`curl -s http://instance-data.ec2.internal`
IP=`ifconfig eth0 | grep 'inet addr:' | grep -v '127.0.0.1' | cut -d: -f2 | awk '{ print $1 }'`

# move into the fluent directory for the rest of the script
cd fluent

# download latest version of the code from relevant repository & branch
git remote remove origin
if [[ -z "$REPO_ORG" ]]; then
  REPO_ORG="fluent-project"
fi

if [[ -z "$REPO_BRANCH" ]]; then
  REPO_BRANCH="master"
fi

# switch to the desired branch; by default we run with master on
# fluent-project/fluent
git remote add origin https://github.com/$REPO_ORG/fluent
git fetch -p origin
git checkout -b brnch origin/$REPO_BRANCH

# generate Python protobufs
cd include/proto
protoc -I=./ --python_out=../../functions/include functions.proto
protoc -I=./ --python_out=../../functions/include kvs.proto
cd ../..

# TODO: this might not be necessary permanently -- depends on whether you're
# changing the client or not
cd kvs/client/python
python3.6 setup.py install --prefix=$HOME/.local
cd ../../..

# start python server
cd functions && export MY_IP=$IP && python3.6 server.py
