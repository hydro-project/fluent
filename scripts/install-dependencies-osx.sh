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

if [ -z "$(command -v brew)" ]; then
  echo "This script requires having homebrew installed."
  echo "Please follow the instructions at https://brew.sh."
  exit 1
fi

if [ -z "$(command -v clang++)" ]; then
  echo "Installing clang via llvm..."
  brew install llvm@5 > /dev/null
fi

if [ ! -f /usr/bin/clang++ ]; then
  echo "Symlinking clang/clang++ to /usr/bin for simplicity..."
  sudo ln -s $(which clang) /usr/bin/clang
  sudo ln -s $(which clang++) /usr/bin/clang++
fi

echo -e "Installing the following packages via homebrew:\n\t* autoconf\n\t* automake\n\t* libtool\n\t* build-essential \n\t* unzip \n\t* pkg-config\n\t* wget"

brew install autoconf automake libtool make unzip pkg-config wget > /dev/null

if [ -z "$(command -v cmake)" ]; then
  echo "Installing cmake..."
  brew install cmake > /dev/null
fi

if [ -z "$(command -v lcov)" ]; then
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
  PROTO_V=3.5.1
  wget https://github.com/google/protobuf/releases/download/v${PROTO_V}/protobuf-all-${PROTO_V}.zip > /dev/null
  unzip protobuf-all-${PROTO_V} > /dev/null
  cd protobuf-${PROTO_V}
  ./autogen.sh && ./configure CXX=clang++ CXXFLAGS='-std=c++11 -stdlib=libc++ -O3 -g'

  make -j4 && sudo make install && sudo update_dyld_shared_cache
  cd .. && rm -rf protobuf-*
fi

echo "All dependencies installed!"
