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

#include "mock/mock_utils.hpp"

MockZmqUtil mock_zmq_util;
ZmqUtilInterface* kZmqUtil = &mock_zmq_util;

MockHashRingUtil mock_hash_ring_util;
HashRingUtilInterface* kHashRingUtil = &mock_hash_ring_util;

std::shared_ptr<spdlog::logger> logger =
    spdlog::basic_logger_mt("mock_logger", "mock_log.txt", true);

class RoutingHandlerTest : public ::testing::Test {
 protected:
  Address ip = "127.0.0.1";
  unsigned thread_id = 0;
  std::unordered_map<unsigned, GlobalHashRing> global_hash_ring_map;
  std::unordered_map<unsigned, LocalHashRing> local_hash_ring_map;
  std::unordered_map<Key, KeyInfo> placement;
  PendingMap<std::pair<Address, std::string>> pending_key_request_map;
  zmq::context_t context;
  SocketCache pushers = SocketCache(&context, ZMQ_PUSH);
  RoutingThread rt;

  RoutingHandlerTest() {
    rt = RoutingThread(ip, thread_id);
    global_hash_ring_map[1].insert(ip, ip, thread_id);
  }

 public:
  void SetUp() {
    // reset all global variables
    kDefaultLocalReplication = 1;
    kDefaultGlobalMemoryReplication = 1;
    kDefaultGlobalEbsReplication = 1;
    kThreadNum = 1;
  }

  void TearDown() {
    // clear all the logged messages after each test
    mock_zmq_util.sent_messages.clear();
  }

  std::vector<std::string> get_zmq_messages() {
    return mock_zmq_util.sent_messages;
  }

  void warmup_placement_to_defaults(std::vector<std::string> keys) {
    for (std::string key : keys) {
      placement[key].global_replication_map_[1] =
          kDefaultGlobalMemoryReplication;
      placement[key].global_replication_map_[2] = kDefaultGlobalEbsReplication;
      placement[key].local_replication_map_[1] = kDefaultLocalReplication;
      placement[key].local_replication_map_[2] = kDefaultLocalReplication;
    }
  }
};
