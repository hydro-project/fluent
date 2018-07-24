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

TEST_F(RoutingHandlerTest, Seed) {
  EXPECT_EQ(global_hash_ring_map[1].size(), 3000);

  auto start_time = std::chrono::system_clock::now();
  auto start_time_ms =
      std::chrono::time_point_cast<std::chrono::milliseconds>(start_time);
  auto value = start_time_ms.time_since_epoch();
  unsigned long long duration = value.count();

  std::string serialized = seed_handler(logger, global_hash_ring_map, duration);

  EXPECT_EQ(global_hash_ring_map[1].size(), 3000);

  TierMembership membership;
  membership.ParseFromString(serialized);

  // check serialized duration
  unsigned long long resp_duration = membership.start_time();
  EXPECT_EQ(resp_duration, duration);

  // check serialized tier size, tier_id, ip
  EXPECT_EQ(membership.tiers_size(), 1);
  for (const auto& tier : membership.tiers()) {
    for (const std::string& other_ip : tier.ips()) {
      EXPECT_EQ(tier.tier_id(), 1);
      EXPECT_EQ(other_ip, ip);
    }
  }
}