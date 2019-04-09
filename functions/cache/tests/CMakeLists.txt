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

SET(CAUSAL_SRC_DIR ${CMAKE_SOURCE_DIR}/functions/cache/src)

FILE(GLOB CAUSAL_SOURCES "${CAUSAL_SRC_DIR}/*handler.cpp" "${CAUSAL_SRC_DIR}/*handlers.cpp" "${CAUSAL_SRC_DIR}/*utils.cpp")
FILE(GLOB MOCK_SOURCES "${CMAKE_SOURCE_DIR}/kvs/tests/mock/*.cpp")

ADD_EXECUTABLE(run_causal_cache_tests
         run_causal_cache_tests.cpp
         ${MOCK_SOURCES}
         ${CAUSAL_SOURCES})

TARGET_LINK_LIBRARIES(run_causal_cache_tests ${FUNC_DEPENDENCIES} gtest gmock pthread zmq)
ADD_DEPENDENCIES(run_causal_cache_tests gtest)

ADD_TEST(NAME CausalTests COMMAND run_causal_cache_tests)
