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

#ifndef KVS_TESTS_MOCKED_HPP_
#define KVS_TESTS_MOCKED_HPP_

#include "kvs/include/hash_ring.hpp"
#include "zmq/zmq_util.hpp"

class MockZmqUtil : public ZmqUtilInterface {
 public:
  vector<string> sent_messages;

  virtual void send_string(const string& s, zmq::socket_t* socket);
  virtual string recv_string(zmq::socket_t* socket);
  virtual int poll(long timeout, vector<zmq::pollitem_t>* items);
};

class MockHashRingUtil : public HashRingUtilInterface {
 public:
  virtual ServerThreadList get_responsible_threads(
      Address respond_address, const Key& key, bool metadata,
      map<TierId, GlobalHashRing>& global_hash_rings,
      map<TierId, LocalHashRing>& local_hash_rings,
      map<Key, KeyReplication>& key_replication_map, SocketCache& pushers,
      const vector<unsigned>& tier_ids, bool& succeed, unsigned& seed);
};

#endif  // KVS_TESTS_MOCKED_HPP_
