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

CMAKE_MINIMUM_REQUIRED(VERSION 3.6 FATAL_ERROR)
PROJECT(Fluent-Function)

SET(FLUENT_FUNC_MAJOR_VERSION ${FLUENT_MAJOR_VERSION})
SET(FLUENT_FUNC_MINOR_VERSION ${FLUENT_MINOR_VERSION})
SET(FLUENT_FUNC_PATCH_VERSION ${FLUENT_PATCH_VERSION})

INCLUDE_DIRECTORIES(${CMAKE_CURRENT_BINARY_DIR})
INCLUDE_DIRECTORIES(include)
INCLUDE_DIRECTORIES(tests)

# INCLUDE(FindProtobuf)
# FIND_PACKAGE(Protobuf REQUIRED)
# INCLUDE_DIRECTORIES(${PROTOBUF_INCLUDE_DIR})
# PROTOBUF_GENERATE_CPP(PROTO_SRC PROTO_HEADER ./include/proto/functions.proto)

# need to build a target at this level or subdirs won't have the 
# protobuf files generated.
# ADD_LIBRARY(flfunc-proto ${PROTO_HEADER} ${PROTO_SRC})

LINK_DIRECTORIES(${ZEROMQ_LINK_DIRS} ${YAMLCPP_LINK_DIRS})

ADD_SUBDIRECTORY(cache)
