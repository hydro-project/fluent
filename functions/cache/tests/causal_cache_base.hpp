//  Copyright 2018 U.C. Berkeley RISE Lab
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.

#include "causal_cache_utils.hpp"
#include "kvs/tests/mock/mock_utils.hpp"

MockZmqUtil mock_zmq_util;
ZmqUtilInterface* kZmqUtil = &mock_zmq_util;

logger log_ = spdlog::basic_logger_mt("mock_log", "mock_log.txt", true);

class CausalCacheTest : public ::testing::Test {
 protected:
  CausalCacheTest() {}

  virtual ~CausalCacheTest() {}

 public:
  void SetUp() {}

  void TearDown() {
    // clear all the logged messages after each test
    mock_zmq_util.sent_messages.clear();
  }

  vector<string> get_zmq_messages() { return mock_zmq_util.sent_messages; }
};
