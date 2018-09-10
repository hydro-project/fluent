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

PROTO_V=3.5.1
LCOV_VERSION=1.13

if [ -z "$(command -v clang++)" ]; then
  echo "Installing clang..."

  sudo apt-add-repository "deb http://apt.llvm.org/trusty/ llvm-toolchain-trusty-5.0 main" > /dev/null
  sudo apt-get update > /dev/null
  sudo apt-get install -y --force-yes clang-5.0 lldb-5.0 clang-format-5.0
  sudo update-alternatives --install /usr/bin/clang clang /usr/bin/clang-5.0 1
  sudo update-alternatives --install /usr/bin/clang++ clang++ /usr/bin/clang++-5.0 1
  sudo update-alternatives --install /usr/bin/clang-format clang-format /usr/bin/clang-format-5.0 1
fi

echo -e "Installing the following packages via apt-get:\n\t* autoconf\n\t* automake\n\t* libtool\n\t* build-essential \n\t* unzip \n\t* pkg-config\n\t* wget\n\t* make\n\t* libc++-dev\n\t* libc++abi-dev"

sudo apt-get install -y build-essential autoconf automake libtool make unzip pkg-config wget make libc++-dev libc++abi-dev > /dev/null

if [ -z "$(command -v cmake)" ]; then
  echo "Installing cmake..."
  echo "You might be prompted for your password to add CMake to /usr/bin."
  wget https://cmake.org/files/v3.11/cmake-3.11.4-Linux-x86_64.tar.gz
  tar xvzf cmake-3.11.4-Linux-x86_64.tar.gz

  sudo mkdir /usr/bin/cmake
  sudo mv cmake-3.11.4-Linux-x86_64/* /usr/bin/cmake/
  echo "export PATH=$PATH:/usr/bin/cmake/bin" >> ~/.bashrc
  source ~/.bashrc

  rm -rf cmake-3.11.4-Linux-x86_64*
fi

if [ -z "$(command -v lcov)" ]; then
  echo "Installing lcov..."
  echo "You might be asked for your password to install lcov..."

  wget http://downloads.sourceforge.net/ltp/lcov-${LCOV_VERSION}.tar.gz
  tar xvzf lcov-${LCOV_VERSION}.tar.gz > /dev/null 2>&1
  rm -rf lcov-${LCOV_VERSION}.tar.gz

  LCOV_DIR="lcov-${LCOV_VERSION}"

  cd $LCOV_DIR && sudo make install
  which lcov
  lcov -v
  cd .. && rm -rf $LCOV_DIR
fi

if [ -z "$(command -v protoc)" ]; then
  echo "Installing protobuf..."
  echo "You might be prompted for your password to install the protobuf headers and set ldconfig."

  wget https://github.com/google/protobuf/releases/download/v${PROTO_V}/protobuf-all-${PROTO_V}.zip > /dev/null
  unzip protobuf-all-${PROTO_V} > /dev/null
  cd protobuf-${PROTO_V}
  ./autogen.sh && ./configure CXX=clang++ CXXFLAGS='-std=c++11 -stdlib=libc++ -O3 -g'

  make -j4 && sudo make install && sudo ldconfig
  cd .. && rm -rf protobuf-3.5.1*
fi

echo "All dependencies installed!"
