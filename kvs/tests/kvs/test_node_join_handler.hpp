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

TEST_F(ServerHandlerTest, BasicNodeJoin) {
  unsigned seed = 0;
  kThreadNum = 2;
  set<Key> join_remove_set;
  AddressKeysetMap join_gossip_map;

  EXPECT_EQ(global_hash_rings[kMemoryTierId].size(), 3000);
  EXPECT_EQ(global_hash_rings[kMemoryTierId].get_unique_servers().size(), 1);

  string serialized = std::to_string(kMemoryTierId) + ":127.0.0.2:127.0.0.2:0";
  node_join_handler(thread_id, seed, ip, ip, log_, serialized,
                    global_hash_rings, local_hash_rings, stored_key_map,
                    key_replication_map, join_remove_set, pushers, wt,
                    join_gossip_map, 0);

  vector<string> messages = get_zmq_messages();
  EXPECT_EQ(messages.size(), 2);
  EXPECT_EQ(messages[0],
            std::to_string(kSelfTierId) + ":" + ip + ":" + ip + ":0");
  EXPECT_EQ(messages[1], serialized);

  EXPECT_EQ(global_hash_rings[kMemoryTierId].size(), 6000);
  EXPECT_EQ(global_hash_rings[kMemoryTierId].get_unique_servers().size(), 2);
}

TEST_F(ServerHandlerTest, DuplicateNodeJoin) {
  unsigned seed = 0;
  set<Key> join_remove_set;
  AddressKeysetMap join_gossip_map;

  EXPECT_EQ(global_hash_rings[kMemoryTierId].size(), 3000);
  EXPECT_EQ(global_hash_rings[kMemoryTierId].get_unique_servers().size(), 1);

  string serialized =
      std::to_string(kMemoryTierId) + ":" + ip + ":" + ip + ":0";
  node_join_handler(thread_id, seed, ip, ip, log_, serialized,
                    global_hash_rings, local_hash_rings, stored_key_map,
                    key_replication_map, join_remove_set, pushers, wt,
                    join_gossip_map, 0);

  vector<string> messages = get_zmq_messages();
  EXPECT_EQ(messages.size(), 0);
  EXPECT_EQ(global_hash_rings[kMemoryTierId].size(), 3000);
  EXPECT_EQ(global_hash_rings[kMemoryTierId].get_unique_servers().size(), 1);
}
