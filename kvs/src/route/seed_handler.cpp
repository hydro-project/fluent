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

std::string seed_handler(
    std::shared_ptr<spdlog::logger> logger,
    std::unordered_map<unsigned, GlobalHashRing>& global_hash_ring_map) {
  logger->info("Received an address request.");

  TierMembership membership;

  for (const auto& global_pair : global_hash_ring_map) {
    unsigned tier_id = global_pair.first;
    auto hash_ring = global_pair.second;

    TierMembership_Tier* tier = membership.add_tiers();
    tier->set_tier_id(tier_id);

    for (const ServerThread& st : hash_ring.get_unique_servers()) {
      auto server = tier->add_servers();
      server->set_private_ip(st.get_private_ip());
      server->set_public_ip(st.get_public_ip());
    }
  }

  std::string serialized;
  membership.SerializeToString(&serialized);
  return serialized;
}
