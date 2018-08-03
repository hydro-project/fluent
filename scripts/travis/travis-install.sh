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

download_protobuf() {
  rm -rf $PROTOBUF_DIR

  wget https://github.com/google/protobuf/releases/download/v3.5.1/protobuf-all-3.5.1.zip
  unzip protobuf-all-3.5.1.zip > /dev/null 2>&1
  mv protobuf-3.5.1 $PROTOBUF_DIR
  rm protobuf-all-3.5.1.zip

  cd $PROTOBUF_DIR && ./autogen.sh
  cd $PROTOBUF_DIR && ./configure CXX=clang++ CXXFLAGS='-std=c++11 -stdlib=libc++ -O3 -g'
}

install_protobuf() {
  cd $PROTOBUF_DIR && make -j4

  cd $PROTOBUF_DIR && sudo make install
  cd $PROTOBUF_DIR && sudo ldconfig
}

download_lcov() {
  wget http://downloads.sourceforge.net/ltp/lcov-${LCOV_VERSION}.tar.gz
  tar xvzf lcov-${LCOV_VERSION}.tar.gz > /dev/null 2>&1
  rm -rf lcov-${LCOV_VERSION}.tar.gz
}

install_lcov() {
  cd $LCOV_DIR && sudo make install
  which lcov
  lcov -v
}

sudo apt-get update
sudo apt-get install -y build-essential autoconf automake libtool curl make unzip pkg-config wget
sudo apt-get install -y libc++-dev libc++abi-dev awscli jq

sudo ln -s $(which clang) /usr/bin/clang
sudo ln -s $(which clang++) /usr/bin/clang++

# if the protobuf directory doesn't exist or is empty
if [ ! -d "$PROTOBUF_DIR" ] || [ -z "$(ls -A $PROTOBUF_DIR)" ] ; then
  download_protobuf
fi

LCOV_DIR="lcov-${LCOV_VERSION}"

install_protobuf

cd $HOME

download_lcov
install_lcov
