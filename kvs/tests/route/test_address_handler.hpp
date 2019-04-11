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

#include "route/routing_handlers.hpp"

TEST_F(RoutingHandlerTest, Address) {
  EXPECT_EQ(global_hash_rings[kMemoryTierId].size(), 3000);

  unsigned seed = 0;

  KeyAddressRequest req;
  req.set_request_id("1");
  req.set_response_address("tcp://127.0.0.1:5000");
  req.add_keys("key");

  string serialized;
  req.SerializeToString(&serialized);

  address_handler(log_, serialized, pushers, rt, global_hash_rings,
                  local_hash_rings, key_replication_map, pending_requests,
                  seed);

  vector<string> messages = get_zmq_messages();

  EXPECT_EQ(messages.size(), 1);
  string serialized_resp = messages[0];

  KeyAddressResponse resp;
  resp.ParseFromString(serialized_resp);

  EXPECT_EQ(resp.response_id(), "1");
  EXPECT_EQ(resp.error(), 0);

  for (const KeyAddressResponse_KeyAddress& addr : resp.addresses()) {
    string key = addr.key();
    EXPECT_EQ(key, "key");
    for (const string& ip : addr.ips()) {
      EXPECT_EQ(ip, "tcp://127.0.0.1:6200");
    }
  }
}
