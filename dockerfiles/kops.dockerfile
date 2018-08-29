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

FROM ubuntu:14.04

MAINTAINER Vikram Sreekanti <vsreekanti@gmail..com> version: 0.1

USER root

# update and install software
RUN apt-get update
RUN apt-get install -y wget curl python3 python3-pip vim jq git
RUN pip3 install awscli
RUN pip3 install zmq

# install kops
RUN wget -O kops https://github.com/kubernetes/kops/releases/download/$(curl -s https://api.github.com/repos/kubernetes/kops/releases/latest | grep -Po '"tag_name": "\K.*?(?=")')/kops-linux-amd64
RUN chmod +x ./kops
RUN mv ./kops /usr/local/bin/

# install kubectl
RUN wget -O kubectl https://storage.googleapis.com/kubernetes-release/release/$(curl -s https://storage.googleapis.com/kubernetes-release/release/stable.txt)/bin/linux/amd64/kubectl
RUN chmod +x ./kubectl
RUN mv ./kubectl /usr/local/bin/kubectl

# NOTE: Doesn't make sense to set up the kops user at build time because we
# need the user's AWS creds... should have a script to do this at runtime
# eventually; for now, going to assume that the user is already set up or we
# can just provide a script to this generally, independent of running it here

RUN git clone https://github.com/fluent-project/fluent
COPY start_kops.sh /fluent/k8s/start_kops.sh

# make kube root dir
RUN mkdir /root/.kube

CMD sh fluent/k8s/start_kops.sh
