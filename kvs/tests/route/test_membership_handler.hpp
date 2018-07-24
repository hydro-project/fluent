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

TEST_F(RoutingHandlerTest, Membership) {
  EXPECT_EQ(global_hash_ring_map[1].size(), 3000);
  EXPECT_EQ(global_hash_ring_map[1].get_unique_servers().size(), 1);

  std::string serialized = "join:1:127.0.0.2";
  membership_handler(logger, serialized, pushers, global_hash_ring_map,
                     thread_id, ip);

  std::vector<std::string> messages = get_zmq_messages();

  EXPECT_EQ(messages.size(), 1);
  EXPECT_EQ(messages[0], "1:127.0.0.2");

  EXPECT_EQ(global_hash_ring_map[1].size(), 6000);
  EXPECT_EQ(global_hash_ring_map[1].get_unique_servers().size(), 2);
}