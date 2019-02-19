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

TEST_F(ServerHandlerTest, SimpleNodeDepart) {
  kThreadNum = 2;
  global_hash_rings[kMemoryTierId].insert("127.0.0.2", "127.0.0.2", 0, 0);

  EXPECT_EQ(global_hash_rings[kMemoryTierId].size(), 6000);
  EXPECT_EQ(global_hash_rings[kMemoryTierId].get_unique_servers().size(), 2);

  string serialized = std::to_string(kMemoryTierId) + ":127.0.0.2:127.0.0.2";
  node_depart_handler(thread_id, ip, ip, global_hash_rings, log_, serialized,
                      pushers);

  vector<string> messages = get_zmq_messages();

  EXPECT_EQ(messages.size(), 1);
  EXPECT_EQ(messages[0], serialized);

  EXPECT_EQ(global_hash_rings[kMemoryTierId].size(), 3000);
  EXPECT_EQ(global_hash_rings[kMemoryTierId].get_unique_servers().size(), 1);
}

TEST_F(ServerHandlerTest, FakeNodeDepart) {
  EXPECT_EQ(global_hash_rings[kMemoryTierId].size(), 3000);
  EXPECT_EQ(global_hash_rings[kMemoryTierId].get_unique_servers().size(), 1);

  string serialized = std::to_string(kMemoryTierId) + ":127.0.0.2:127.0.0.2";
  node_depart_handler(thread_id, ip, ip, global_hash_rings, log_, serialized,
                      pushers);

  vector<string> messages = get_zmq_messages();

  EXPECT_EQ(messages.size(), 0);

  EXPECT_EQ(global_hash_rings[kMemoryTierId].size(), 3000);
  EXPECT_EQ(global_hash_rings[kMemoryTierId].get_unique_servers().size(), 1);
}
