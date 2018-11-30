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

# update and install python3
RUN apt-get update
RUN apt-get install -y vim curl wget git gcc libzmq-dev python-software-properties software-properties-common
RUN add-apt-repository -y ppa:jonathonf/python-3.6
RUN apt-get update
RUN apt-get install -y python3.6
RUN wget https://bootstrap.pypa.io/get-pip.py
RUN python3.6 get-pip.py
RUN pip3 install flask cloudpickle zmq protobuf Flask-Session

# clone and install relevant libraries
RUN git clone https://github.com/$repo_org/fluent
WORKDIR /fluent
RUN git fetch origin && git checkout -b $build_branch origin/$source_branch
RUN cd client/python && python3.6 setup.py install --prefix=$HOME/.local
WORKDIR /

COPY start-funcs.sh /start-funcs.sh

# start the Flask server
CMD bash start-funcs.sh
