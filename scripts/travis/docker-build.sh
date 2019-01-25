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

# only build a new Docker image if this is a master branch build -- ignore this
# for PR builds
if [[ "$TRAVIS_BRANCH" == "master" ]]; then
  cd dockerfiles
  docker build . -f anna.dockerfile -t fluentproject/annakvs
  docker build . -f kops.dockerfile -t fluentproject/kops

  echo "$DOCKER_PASSWORD" | docker login -u "$DOCKER_USERNAME" --password-stdin

  docker push fluentproject/annakvs
  docker push fluentproject/kops
fi
