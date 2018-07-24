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

SET(KVS_SRC_DIR ${CMAKE_SOURCE_DIR}/kvs/src/kvs)

FILE(GLOB HANDLER_SOURCES "${KVS_SRC_DIR}/*handler.cpp")
FILE(GLOB MOCK_SOURCES "${CMAKE_SOURCE_DIR}/kvs/tests/mock/*.cpp")

ADD_EXECUTABLE(run_server_handler_tests
         run_server_handler_tests.cpp
         ${MOCK_SOURCES}
         ${HANDLER_SOURCES}
         ${KVS_SRC_DIR}/utils.cpp)

TARGET_LINK_LIBRARIES(run_server_handler_tests gtest gmock pthread flkvs-ring zmq)
ADD_DEPENDENCIES(run_server_handler_tests gtest)

ADD_TEST(NAME ServerTests COMMAND run_server_handler_tests)
