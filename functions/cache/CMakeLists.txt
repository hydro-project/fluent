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


INCLUDE_DIRECTORIES(${CMAKE_CURRENT_BINARY_DIR})
INCLUDE_DIRECTORIES(include)

SET(FUNC_DEPENDENCIES
    ${PROTO_SRC}
    ${PROTO_HEADER}
)

SET(FUNC_DEPENDENCIES
    protobuf
    flproto
    pthread
    zmq
    flzmq
    yaml-cpp
)

ADD_SUBDIRECTORY(src)
ADD_SUBDIRECTORY(tests)
