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

std::string kRequestId = "0";

class ServerHandlerTest : public ::testing::Test {
 protected:
  Address ip = "127.0.0.1";
  unsigned thread_id = 0;
  std::unordered_map<unsigned, GlobalHashRing> global_hash_ring_map;
  std::unordered_map<unsigned, LocalHashRing> local_hash_ring_map;
  std::unordered_map<Key, KeyInfo> placement;
  std::unordered_map<Key, unsigned> key_size_map;
  ServerThread wt;
  PendingMap<PendingRequest> pending_request_map;
  PendingMap<PendingGossip> pending_gossip_map;
  std::unordered_map<
      Key, std::multiset<std::chrono::time_point<std::chrono::system_clock>>>
      key_access_timestamp;
  std::unordered_set<Key> local_changeset;

  zmq::context_t context;
  SocketCache pushers = SocketCache(&context, ZMQ_PUSH);
  Serializer* serializer;
  MemoryKVS* kvs;

  ServerHandlerTest() {
    kvs = new MemoryKVS();
    serializer = new MemorySerializer(kvs);
    wt = ServerThread(ip, thread_id);
    global_hash_ring_map[1].insert(ip, thread_id);
  }

  virtual ~ServerHandlerTest() {
    delete kvs;
    delete serializer;
  }

 public:
  void SetUp() {
    // reset all global variables
    kDefaultLocalReplication = 1;
    kSelfTierId = 1;
    kThreadNum = 1;
    kSelfTierIdVector = {kSelfTierId};
  }

  void TearDown() {
    // clear all the logged messages after each test
    mock_zmq_util.sent_messages.clear();
  }

  std::vector<std::string> get_zmq_messages() {
    return mock_zmq_util.sent_messages;
  }

  // NOTE: Pass in an empty string to avoid putting something into the
  // serializer
  std::string get_key_request(Key key, std::string ip) {
    KeyRequest request;
    request.set_type(get_request_type("GET"));
    request.set_response_address(
        UserThread(ip, 0).get_request_pulling_connect_addr());
    request.set_request_id(kRequestId);

    KeyTuple* tp = request.add_tuples();
    tp->set_key(key);

    std::string request_str;
    request.SerializeToString(&request_str);

    return request_str;
  }

  std::string put_key_request(Key key, std::string value, std::string ip) {
    KeyRequest request;
    request.set_type(get_request_type("PUT"));
    request.set_response_address(
        UserThread(ip, 0).get_request_pulling_connect_addr());
    request.set_request_id(kRequestId);

    KeyTuple* tp = request.add_tuples();
    tp->set_key(key);
    tp->set_value(value);
    tp->set_timestamp(0);

    std::string request_str;
    request.SerializeToString(&request_str);

    return request_str;
  }
};
