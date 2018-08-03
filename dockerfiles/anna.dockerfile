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

# run updates
RUN apt-get update
RUN apt-get install -y build-essential autoconf automake libtool curl make unzip pkg-config wget git vim awscli jq software-properties-common
RUN sudo apt-add-repository "deb http://apt.llvm.org/trusty/ llvm-toolchain-trusty-5.0 main"
RUN apt-get update

# this uses --force-yes because of some disk space warning
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

# install protobuf
RUN wget https://github.com/google/protobuf/releases/download/v3.5.1/protobuf-all-3.5.1.zip
RUN unzip protobuf-all-3.5.1.zip 

WORKDIR /protobuf-3.5.1/
RUN ./autogen.sh
RUN ./configure CXX=clang++ CXXFLAGS='-std=c++11 -stdlib=libc++ -O3 -g'
RUN make -j4
RUN make check -j4
RUN make install
RUN ldconfig

WORKDIR /
RUN rm -rf protobuf-3.5.1 protobuf-all-3.5.1.zip

# build Bedrock
RUN git clone https://github.com/fluent-project/fluent
RUN cd fluent && bash scripts/build-all.sh -j4 -bRelease
COPY start.sh /fluent/k8s/start.sh

CMD bash fluent/k8s/start.sh $SERVER_TYPE
