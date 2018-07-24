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

#include "kvs/kvs_handlers.hpp"

TEST_F(ServerHandlerTest, SimpleGossipReceive) {
  Key key = "key";
  std::string value = "value";
  std::string put_request = put_key_request(key, value, ip);

  unsigned total_access = 0;
  unsigned seed = 0;
  unsigned error;
  auto now = std::chrono::system_clock::now();

  EXPECT_EQ(local_changeset.size(), 0);

  gossip_handler(seed, put_request, global_hash_ring_map, local_hash_ring_map,
                 key_size_map, pending_gossip_map, placement, wt, serializer,
                 pushers);

  EXPECT_EQ(pending_gossip_map.size(), 0);
  EXPECT_EQ(serializer->get(key, error).reveal().value, value);
}

TEST_F(ServerHandlerTest, GossipUpdate) {
  Key key = "key";
  std::string value = "value1";
  serializer->put(key, value, (unsigned)0);

  value = "value2";

  std::string put_request = put_key_request(key, value, ip);

  unsigned total_access = 0;
  unsigned seed = 0;
  unsigned error;
  auto now = std::chrono::system_clock::now();

  EXPECT_EQ(local_changeset.size(), 0);

  gossip_handler(seed, put_request, global_hash_ring_map, local_hash_ring_map,
                 key_size_map, pending_gossip_map, placement, wt, serializer,
                 pushers);

  EXPECT_EQ(pending_gossip_map.size(), 0);
  EXPECT_EQ(serializer->get(key, error).reveal().value, value);
}

// TODO: test pending gossip
// TODO: test gossip forwarding
