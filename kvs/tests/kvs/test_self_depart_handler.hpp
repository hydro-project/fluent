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

TEST_F(ServerHandlerTest, SelfDepart) {
  unsigned seed = 0;
  std::vector<Address> routing_address;
  std::vector<Address> monitoring_address;

  EXPECT_EQ(global_hash_ring_map[1].size(), 3000);
  EXPECT_EQ(global_hash_ring_map[1].get_unique_servers().size(), 1);

  std::string serialized = "tcp://127.0.0.2:6560";

  self_depart_handler(thread_id, seed, ip, logger, serialized,
                      global_hash_ring_map, local_hash_ring_map, key_size_map,
                      placement, routing_address, monitoring_address, wt,
                      pushers, serializer);

  EXPECT_EQ(global_hash_ring_map[1].size(), 0);
  EXPECT_EQ(global_hash_ring_map[1].get_unique_servers().size(), 0);

  std::vector<std::string> zmq_messages = get_zmq_messages();
  EXPECT_EQ(zmq_messages.size(), 1);
  EXPECT_EQ(zmq_messages[0], ip + "_" + std::to_string(kSelfTierId));
}

// TODO: test should add keys and make sure that they are gossiped elsewhere
// TODO: test should make sure that depart messages are sent to the worker
// threads
