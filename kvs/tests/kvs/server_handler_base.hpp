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

logger log_ = spdlog::basic_logger_mt("mock_log", "mock_log.txt", true);

string kRequestId = "0";

class ServerHandlerTest : public ::testing::Test {
 protected:
  Address ip = "127.0.0.1";
  unsigned thread_id = 0;
  map<TierId, GlobalHashRing> global_hash_rings;
  map<TierId, LocalHashRing> local_hash_rings;
  map<Key, KeyProperty> stored_key_map;
  map<Key, KeyReplication> key_replication_map;
  ServerThread wt;
  map<Key, vector<PendingRequest>> pending_requests;
  map<Key, vector<PendingGossip>> pending_gossip;
  map<Key, std::multiset<TimePoint>> key_access_tracker;
  set<Key> local_changeset;

  zmq::context_t context;
  SocketCache pushers = SocketCache(&context, ZMQ_PUSH);
  SerializerMap serializers;
  Serializer* lww_serializer;
  Serializer* set_serializer;
  Serializer* causal_serializer;
  MemoryLWWKVS* lww_kvs;
  MemorySetKVS* set_kvs;
  MemoryCausalKVS* causal_kvs;

  ServerHandlerTest() {
    lww_kvs = new MemoryLWWKVS();
    lww_serializer = new MemoryLWWSerializer(lww_kvs);
    set_kvs = new MemorySetKVS();
    set_serializer = new MemorySetSerializer(set_kvs);
    causal_kvs = new MemoryCausalKVS();
    causal_serializer = new MemoryCausalSerializer(causal_kvs);
    serializers[LatticeType::LWW] = lww_serializer;
    serializers[LatticeType::SET] = set_serializer;
    serializers[LatticeType::CAUSAL] = causal_serializer;

    wt = ServerThread(ip, ip, thread_id);
    global_hash_rings[kMemoryTierId].insert(ip, ip, 0, thread_id);
  }

  virtual ~ServerHandlerTest() {
    delete lww_kvs;
    delete set_kvs;
    delete serializers[LatticeType::LWW];
    delete serializers[LatticeType::SET];
  }

 public:
  void SetUp() {
    // reset all global variables
    kDefaultLocalReplication = 1;
    kSelfTierId = kMemoryTierId;
    kThreadNum = 1;
    kSelfTierIdVector = {kSelfTierId};
  }

  void TearDown() {
    // clear all the logged messages after each test
    mock_zmq_util.sent_messages.clear();
  }

  vector<string> get_zmq_messages() { return mock_zmq_util.sent_messages; }

  // NOTE: Pass in an empty string to avoid putting something into the
  // serializer
  string get_key_request(Key key, string ip) {
    KeyRequest request;
    request.set_type(RequestType::GET);
    request.set_response_address(UserThread(ip, 0).response_connect_address());
    request.set_request_id(kRequestId);

    KeyTuple* tp = request.add_tuples();
    tp->set_key(std::move(key));

    string request_str;
    request.SerializeToString(&request_str);

    return request_str;
  }

  string put_key_request(Key key, LatticeType lattice_type, string payload,
                         string ip) {
    KeyRequest request;
    request.set_type(RequestType::PUT);
    request.set_response_address(UserThread(ip, 0).response_connect_address());
    request.set_request_id(kRequestId);

    KeyTuple* tp = request.add_tuples();
    tp->set_key(std::move(key));
    tp->set_lattice_type(std::move(lattice_type));
    tp->set_payload(std::move(payload));

    string request_str;
    request.SerializeToString(&request_str);

    return request_str;
  }
};
