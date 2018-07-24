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

#include "mock_utils.hpp"

void MockZmqUtil::send_string(const std::string& s, zmq::socket_t* socket) {
  sent_messages.push_back(s);
}

std::string MockZmqUtil::recv_string(zmq::socket_t* socket) { return ""; }

int MockZmqUtil::poll(long timeout, std::vector<zmq::pollitem_t>* items) {
  return 0;
}

// get all threads responsible for a key from the "node_type" tier
// metadata flag = 0 means the key is  metadata; otherwise, it is  regular data
ServerThreadSet MockHashRingUtil::get_responsible_threads(
    Address respond_address, const Key& key, bool metadata,
    std::unordered_map<unsigned, GlobalHashRing>& global_hash_ring_map,
    std::unordered_map<unsigned, LocalHashRing>& local_hash_ring_map,
    std::unordered_map<Key, KeyInfo>& placement, SocketCache& pushers,
    const std::vector<unsigned>& tier_ids, bool& succeed, unsigned& seed) {
  ServerThreadSet threads;
  succeed = true;

  threads.insert(ServerThread("127.0.0.1", 0));
  return threads;
}
