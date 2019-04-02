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

ARG repo_org=fluent-project
ARG source_branch=master
ARG build_branch=docker-build

USER root

# update and install software
RUN apt-get update -y
RUN apt-get install -y vim curl jq wget git libpq-dev libssl-dev openssl libffi-dev zlib1g-dev python-software-properties software-properties-common build-essential autoconf automake libtool make unzip
RUN apt-add-repository "deb http://apt.llvm.org/trusty/ llvm-toolchain-trusty-5.0 main"
RUN add-apt-repository -y ppa:jonathonf/python-3.6
RUN apt-get update
RUN apt-get install -y python3.6
RUN wget https://bootstrap.pypa.io/get-pip.py
RUN python3.6 get-pip.py
RUN pip3 install awscli zmq six kubernetes boto3 protobuf

# this uses --force-yes because of some disk space warning
RUN sudo apt-get install -y clang-5.0 lldb-5.0 --force-yes
RUN update-alternatives --install /usr/bin/clang clang /usr/bin/clang-5.0 1
RUN update-alternatives --install /usr/bin/clang++ clang++ /usr/bin/clang++-5.0 1
RUN apt-get install -y libc++-dev libc++abi-dev 

# download protobuf
RUN wget https://github.com/google/protobuf/releases/download/v3.5.1/protobuf-all-3.5.1.zip
RUN unzip protobuf-all-3.5.1.zip 

# install protobuf
WORKDIR /protobuf-3.5.1/
RUN ./autogen.sh
RUN ./configure CXX=clang++ CXXFLAGS='-std=c++11 -stdlib=libc++ -O3 -g'
RUN make -j4
RUN make check -j4
RUN make install
RUN ldconfig

WORKDIR /
RUN rm -rf protobuf-3.5.1 protobuf-all-3.5.1.zip

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
RUN git clone https://github.com/$repo_org/fluent
RUN cd fluent && git fetch origin && git checkout -b $build_branch origin/$source_branch

# make kube root dir
RUN mkdir /root/.kube

COPY start_kops.sh /
CMD bash start_kops.sh
