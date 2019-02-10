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

TEST_F(RoutingHandlerTest, ReplicationResponse) {
  unsigned seed = 0;
  std::string key = "key";
  std::vector<std::string> keys = {"key"};
  warmup_placement_to_defaults(keys);

  EXPECT_EQ(placement[key].global_replication_map_[1], 1);
  EXPECT_EQ(placement[key].global_replication_map_[2], 1);
  EXPECT_EQ(placement[key].local_replication_map_[1], 1);
  EXPECT_EQ(placement[key].local_replication_map_[2], 1);

  KeyResponse response;
  KeyTuple* tp = response.add_tuples();
  tp->set_key("|key|replication");
  tp->set_error(0);

  std::string metakey = key;
  ReplicationFactor rf;
  rf.set_key(key);

  for (unsigned i = 1; i < 3; i++) {
    Replication* rep_global = rf.add_global();
    rep_global->set_tier_id(i);
    rep_global->set_replication_factor(2);
  }

  for (unsigned i = 1; i < 3; i++) {
    Replication* rep_local = rf.add_local();
    rep_local->set_tier_id(i);
    rep_local->set_replication_factor(3);
  }

  std::string repfactor;
  rf.SerializeToString(&repfactor);

  tp->set_value(repfactor);

  std::string serialized;
  response.SerializeToString(&serialized);

  replication_response_handler(logger, serialized, pushers, rt,
                               global_hash_ring_map, local_hash_ring_map,
                               placement, pending_key_request_map, seed);

  EXPECT_EQ(placement[key].global_replication_map_[1], 2);
  EXPECT_EQ(placement[key].global_replication_map_[2], 2);
  EXPECT_EQ(placement[key].local_replication_map_[1], 3);
  EXPECT_EQ(placement[key].local_replication_map_[2], 3);
}
