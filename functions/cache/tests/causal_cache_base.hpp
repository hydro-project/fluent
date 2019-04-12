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

#include "kvs_mock_client.hpp"

MockZmqUtil mock_zmq_util;
ZmqUtilInterface* kZmqUtil = &mock_zmq_util;

KvsMockClient mock_cl;
KvsAsyncClientInterface* client = &mock_cl;

logger log_ = spdlog::basic_logger_mt("mock_log", "mock_log.txt", true);

class CausalCacheTest : public ::testing::Test {
 protected:
  zmq::context_t context;
  SocketCache pushers = SocketCache(&context, ZMQ_PUSH);

  // keep track of keys that this causal cache is responsible for
  set<Key> key_set;

  StoreType unmerged_store;
  InPreparationType in_preparation;
  StoreType causal_cut_store;
  VersionStoreType version_store;

  map<Key, set<Key>> to_fetch_map;
  map<Key, std::unordered_map<VectorClock, set<Key>, VectorClockHash>>
      cover_map;

  map<Key, set<Address>> single_callback_map;

  map<Address, PendingClientMetadata> pending_single_metadata;
  map<Address, PendingClientMetadata> pending_cross_metadata;

  // mapping from client id to a set of response address of GET request
  map<string, set<Address>> client_id_to_address_map;

  // mapping from request id to response address of PUT request
  map<string, Address> request_id_to_address_map;

  CausalCacheThread cct = CausalCacheThread("1.1.1.1", 0);

  string value = "test_value";
  Address dummy_address = "tcp://1.1.1.1:1";

  CausalCacheTest() {}

  virtual ~CausalCacheTest() {}

 public:
  void SetUp() {}

  void TearDown() {
    // clear all the logged messages after each test
    mock_zmq_util.sent_messages.clear();
    mock_cl.clear();
  }

  vector<string> get_zmq_messages() { return mock_zmq_util.sent_messages; }

  VectorClock construct_vector_clock(const map<string, unsigned>& vc) {
    VectorClock result;
    for (const auto& pair : vc) {
      result.insert(pair.first, pair.second);
    }
    return result;
  }

  std::shared_ptr<CrossCausalLattice<SetLattice<string>>>
  construct_causal_lattice(const VectorClock& vc,
                           const map<Key, VectorClock>& dep,
                           const string& val) {
    CrossCausalPayload<SetLattice<string>> ccp;
    ccp.vector_clock = vc;
    for (const auto& pair : dep) {
      ccp.dependency.insert(pair.first, pair.second);
    }
    ccp.value.insert(val);

    return std::make_shared<CrossCausalLattice<SetLattice<string>>>(ccp);
  }
};
