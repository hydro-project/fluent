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

#ifndef SRC_INCLUDE_MOCK_CLIENT_HPP_
#define SRC_INCLUDE_MOCK_CLIENT_HPP_

#include "common.hpp"
#include "kvs.pb.h"
#include "requests.hpp"
#include "threads.hpp"
#include "types.hpp"

#include "kvs_async_client.hpp"

class KvsMockClient : public KvsAsyncClientInterface {
 public:
  /**
   * @addrs A vector of routing addresses.
   * @routing_thread_count The number of thread sone ach routing node
   * @ip My node's IP address
   * @tid My client's thread ID
   * @timeout Length of request timeouts in ms
   */
  KvsMockClient() { rid_ = 0; }

  ~KvsMockClient() {}

  /**
   * Issue an async PUT request to the KVS for a certain lattice typed value.
   */
  string put_async(const Key& key, const string& payload,
                   LatticeType lattice_type) {
    keys_put_.push_back(key);
    return get_request_id();
  }

  /**
   * Issue an async GET request to the KVS.
   */
  void get_async(const Key& key) { keys_get_.push_back(key); }

  vector<KeyResponse> receive_async(ZmqUtilInterface* kZmqUtil) {
    return responses_;
  }

  zmq::context_t* get_context() { return nullptr; }

  void clear() {
    keys_put_.clear();
    keys_get_.clear();
    responses_.clear();
  }

  // keep track of the keys being put
  vector<Key> keys_put_;
  // keep track of the keys being get
  vector<Key> keys_get_;
  // responses to send back
  vector<KeyResponse> responses_;

 private:
  /**
   * Generates a unique request ID.
   */
  string get_request_id() {
    if (++rid_ % 10000 == 0) rid_ = 0;
    return std::to_string(rid_++);
  }

  // the current request id
  unsigned rid_;
};

#endif  // SRC_INCLUDE_MOCK_CLIENT_HPP_
