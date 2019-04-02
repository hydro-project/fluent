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

MAINTAINER Vikram Sreekanti <vsreekanti@gmail.com> version: 0.1

USER root

# install relevant apt packages
RUN apt-get update
RUN apt-get install -y build-essential autoconf automake libtool curl make \
      unzip pkg-config wget curl git vim jq software-properties-common \
      libzmq-dev git gcc python-software-properties libpq-dev libssl-dev \
      openssl libffi-dev zlib1g-dev

# install clang-5 and other related C++ things; this uses --force-yes because 
# of some odd disk space warning that I can't get rid of
RUN sudo apt-add-repository \
      "deb http://apt.llvm.org/trusty/ llvm-toolchain-trusty-5.0 main"
RUN apt-get update
RUN sudo apt-get install -y clang-5.0 lldb-5.0 --force-yes
RUN update-alternatives --install /usr/bin/clang clang /usr/bin/clang-5.0 1
RUN update-alternatives --install /usr/bin/clang++ clang++ /usr/bin/clang++-5.0 1
RUN apt-get install -y libc++-dev libc++abi-dev 

# install cmake
RUN wget https://cmake.org/files/v3.9/cmake-3.9.4-Linux-x86_64.tar.gz
RUN tar xvzf cmake-3.9.4-Linux-x86_64.tar.gz
RUN mv cmake-3.9.4-Linux-x86_64 /usr/bin/cmake
ENV PATH $PATH:/usr/bin/cmake/bin
RUN rm cmake-3.9.4-Linux-x86_64.tar.gz

# install python3.6
RUN add-apt-repository -y ppa:jonathonf/python-3.6
RUN apt-get update
RUN apt-get install -y python3.6
RUN wget https://bootstrap.pypa.io/get-pip.py
RUN python3.6 get-pip.py
RUN pip3 install awscli cloudpickle zmq protobuf boto3 kubernetes six

# download protobuf
RUN wget https://github.com/google/protobuf/releases/download/v3.5.1/protobuf-all-3.5.1.zip
RUN unzip protobuf-all-3.5.1.zip 

# build and install protobuf -- this step takes a really long time!
WORKDIR /protobuf-3.5.1/
RUN ./autogen.sh
RUN ./configure CXX=clang++ CXXFLAGS='-std=c++11 -stdlib=libc++ -O3 -g'
RUN make -j4
RUN make check -j4
RUN make install
RUN ldconfig

# clean up protobuf install files
WORKDIR /
RUN rm -rf protobuf-3.5.1 protobuf-all-3.5.1.zip

# clone the fluent repo
RUN git clone https://github.com/fluent-project/fluent
