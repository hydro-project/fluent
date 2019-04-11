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

std::shared_ptr<spdlog::logger> log_ =
    spdlog::basic_logger_mt("mock_log", "mock_log.txt", true);

class RoutingHandlerTest : public ::testing::Test {
 protected:
  Address ip = "127.0.0.1";
  unsigned thread_id = 0;
  map<TierId, GlobalHashRing> global_hash_rings;
  map<TierId, LocalHashRing> local_hash_rings;
  map<Key, KeyReplication> key_replication_map;
  map<Key, vector<pair<Address, string>>> pending_requests;
  zmq::context_t context;
  SocketCache pushers = SocketCache(&context, ZMQ_PUSH);
  RoutingThread rt;

  RoutingHandlerTest() {
    rt = RoutingThread(ip, thread_id);
    global_hash_rings[kMemoryTierId].insert(ip, ip, 0, thread_id);
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

  vector<string> get_zmq_messages() { return mock_zmq_util.sent_messages; }

  void warmup_key_replication_map_to_defaults(vector<string> keys) {
    for (string key : keys) {
      key_replication_map[key].global_replication_[kMemoryTierId] =
          kDefaultGlobalMemoryReplication;
      key_replication_map[key].global_replication_[kEbsTierId] =
          kDefaultGlobalEbsReplication;
      key_replication_map[key].local_replication_[kMemoryTierId] =
          kDefaultLocalReplication;
      key_replication_map[key].local_replication_[kEbsTierId] =
          kDefaultLocalReplication;
    }
  }
};
