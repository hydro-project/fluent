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

# set AWS environment variables
mkdir -p ~/.aws
echo "[default]\nregion = us-east-1" > ~/.aws/config
echo "[default]\naws_access_key_id = $AWS_ACCESS_KEY_ID\naws_secret_access_key = $AWS_SECRET_ACCESS_KEY" > ~/.aws/credentials
mkdir -p ~/.ssh

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

# generate Python protobuf libraries
cd include/proto
protoc -I=./ --python_out=../../k8s --python_out=. kvs.proto
cd ../../kvs/include/proto
protoc -I=./ --python_out=../../../k8s metadata.proto
cd ../../..

# start python server
cd k8s && python3.6 kops_server.py
