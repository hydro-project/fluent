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

if [ ! -z "$(command -v apt-get)" ]; then
  echo "Detected that this is a Debian-based distribution."
  DIST="debian"
  PKG_MGR=apt-get
  sudo $PKG_MGR update -y > /dev/null 2>&1
elif [ ! -z "$(command -v yum)" ]; then
  echo "Detected that this is a Fedora-based distribution."
  DIST="fedora"
  PKG_MGR=yum
  sudo $PKG_MGR update -y > /dev/null 2>&1
else
  echo "Unrecognized Linux distribution -- only Debian and Fedora are currently supported."
  exit 1
fi

if [ -z "$1" ]; then
  echo "No compiler is specified. Default compiler is clang++."
  COMPILER=clang++
else
  if [ "$1" != "clang++" ] && [ "$1" != "g++" ]; then
    echo "$1 is not a supported compiler. Valid options are clang++ or GNU g++."
    exit 1
  fi

  echo "Setting compiler to $1..."
  COMPILER=$1
fi

if [ "$DIST" = "fedora" ] && [ "$COMPILER" = "clang++" ]; then
  echo "We currently are unable to support clang++ installation on Fedora distributions."
  exit 1
fi

if [ "$COMPILER" = "clang++" ] && [ -z "$(command -v clang++)" ]; then
  echo "Installing clang..."

  sudo apt-add-repository "deb http://apt.llvm.org/trusty/ llvm-toolchain-trusty-5.0 main" > /dev/null
  sudo apt-get install -y --force-yes clang-5.0 lldb-5.0 clang-format-5.0 > /dev/null
  sudo update-alternatives --install /usr/bin/clang clang /usr/bin/clang-5.0 1 > /dev/null
  sudo update-alternatives --install /usr/bin/clang++ clang++ /usr/bin/clang++-5.0 1 > /dev/null
  sudo update-alternatives --install /usr/bin/clang-format clang-format /usr/bin/clang-format-5.0 1 > /dev/null
fi

if [ "$COMPILER" = "g++" ] && [ -z "$(command -v g++)" ]; then
  echo "Installing g++..."
  sudo $PKG_MGR install -y gcc g++ > /dev/null

  if [ "$DIST" = "fedora" ]; then
    sudo $PKG_MGR install -y gcc-c++ > /dev/null
  fi
fi


if [ "$DIST" = "debian" ]; then
  echo -e "Installing the following packages via $PKG_MGR:\n\t* autoconf\n\t* automake\n\t* libtool\n\t* build-essential \n\t* unzip \n\t* pkg-config\n\t* wget\n\t* make\n\t* libc++-dev\n\t* libc++abi-dev"
  sudo $PKG_MGR install -y build-essential autoconf automake libtool unzip pkg-config wget make libc++-dev libc++abi-dev > /dev/null
elif [ "$DIST" = "fedora" ]; then
  echo -e "Installing the following packages via $PKG_MGR:\n\t* autoconf\n\t* automake\n\t* libtool\n\t* build-essential \n\t* make"
  sudo $PKG_MGR install -y build-essential autoconf automake libtool make > /dev/null
else
  exit 1
fi


if [ -z "$(command -v cmake)" ]; then
  echo "Installing cmake..."
  echo "You might be prompted for your password to add CMake to /usr/bin."
  wget https://cmake.org/files/v3.11/cmake-3.11.4-Linux-x86_64.tar.gz
  tar xvzf cmake-3.11.4-Linux-x86_64.tar.gz > /dev/null 2>&1

  sudo mkdir /usr/cmake
  sudo mv cmake-3.11.4-Linux-x86_64/* /usr/cmake/
  sudo ln -s /usr/cmake/bin/cmake /usr/bin/cmake

  rm -rf cmake-3.11.4-Linux-x86_64*
fi

if [ -z "$(command -v lcov)" ]; then
  echo "Installing lcov..."
  echo "You might be asked for your password to install lcov..."

  wget http://downloads.sourceforge.net/ltp/lcov-${LCOV_VERSION}.tar.gz
  tar xvzf lcov-${LCOV_VERSION}.tar.gz > /dev/null 2>&1
  rm -rf lcov-${LCOV_VERSION}.tar.gz

  LCOV_DIR="lcov-${LCOV_VERSION}"

  cd $LCOV_DIR
  sudo make install > /dev/null
  cd .. && rm -rf $LCOV_DIR
fi

if [ -z "$(command -v protoc)" ]; then
  echo "Installing protobuf..."
  echo "You might be prompted for your password to install the protobuf headers and set ldconfig."

  wget https://github.com/google/protobuf/releases/download/v${PROTO_V}/protobuf-all-${PROTO_V}.zip > /dev/null
  unzip protobuf-all-${PROTO_V} > /dev/null
  cd protobuf-${PROTO_V}
  ./autogen.sh > /dev/null
  if [ "$COMPILER" = "clang++" ]; then
    ./configure CXX=clang++ CXXFLAGS='-std=c++11 -stdlib=libc++ -O3 -g' > /dev/null
  else
    ./configure CXX=g++ CXXFLAGS='-std=c++11 -O3 -g' > /dev/null
  fi

  make -j4 > /dev/null
  sudo make install > /dev/null
  sudo ldconfig > /dev/null

  if [ "$DIST" = "fedora " ]; then
    # on Fedora, ldconfig doesn't seem to recognize libprotobuf.so for some
    # reason
    export LD_LIBRARY_PATH=/usr/local/lib
    echo "export LD_LIBRARY_PATH=/usr/local/lib" >> ~/.bashrc
    source ~/.bashrc
  fi

  cd .. && rm -rf protobuf-3.5.1*
fi

echo "All dependencies installed!"
