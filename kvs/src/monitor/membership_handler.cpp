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

#include "monitor/monitoring_handlers.hpp"

void membership_handler(
    std::shared_ptr<spdlog::logger> logger, std::string& serialized,
    std::unordered_map<unsigned, GlobalHashRing>& global_hash_ring_map,
    unsigned& adding_memory_node, unsigned& adding_ebs_node,
    std::chrono::time_point<std::chrono::system_clock>& grace_start,
    std::vector<Address>& routing_address, StorageStat& memory_tier_storage,
    StorageStat& ebs_tier_storage, OccupancyStat& memory_tier_occupancy,
    OccupancyStat& ebs_tier_occupancy,
    std::unordered_map<Key, std::unordered_map<Address, unsigned>>&
        key_access_frequency) {
  std::vector<std::string> v;

  split(serialized, ':', v);
  std::string type = v[0];
  unsigned tier = stoi(v[1]);
  Address new_server_ip = v[2];

  if (type == "join") {
    logger->info("Received join from server {} in tier {}.", new_server_ip,
                 std::to_string(tier));
    if (tier == 1) {
      global_hash_ring_map[tier].insert(new_server_ip, 0);

      if (adding_memory_node > 0) {
        adding_memory_node -= 1;
      }

      // reset grace period timer
      grace_start = std::chrono::system_clock::now();
    } else if (tier == 2) {
      global_hash_ring_map[tier].insert(new_server_ip, 0);

      if (adding_ebs_node > 0) {
        adding_ebs_node -= 1;
      }

      // reset grace period timer
      grace_start = std::chrono::system_clock::now();
    } else if (tier == 0) {
      routing_address.push_back(new_server_ip);
    } else {
      logger->error("Invalid tier: {}.", std::to_string(tier));
    }

    for (const auto& global_pair : global_hash_ring_map) {
      logger->info("Hash ring for tier {} is size {}.",
                   std::to_string(global_pair.first),
                   std::to_string(global_pair.second.size()));
    }
  } else if (type == "depart") {
    logger->info("Received depart from server {}.", new_server_ip);

    // update hash ring
    if (tier == 1) {
      global_hash_ring_map[tier].remove(new_server_ip, 0);
      memory_tier_storage.erase(new_server_ip);
      memory_tier_occupancy.erase(new_server_ip);

      // NOTE: No const here because we are calling erase
      for (auto& key_access_pair : key_access_frequency) {
        for (unsigned i = 0; i < kMemoryThreadCount; i++) {
          key_access_pair.second.erase(new_server_ip + ":" + std::to_string(i));
        }
      }
    } else if (tier == 2) {
      global_hash_ring_map[tier].remove(new_server_ip, 0);
      ebs_tier_storage.erase(new_server_ip);
      ebs_tier_occupancy.erase(new_server_ip);

      // NOTE: No const here because we are calling erase
      for (auto& key_access_pair : key_access_frequency) {
        for (unsigned i = 0; i < kEbsThreadCount; i++) {
          key_access_pair.second.erase(new_server_ip + ":" + std::to_string(i));
        }
      }
    } else {
      logger->error("Invalid tier: {}.", std::to_string(tier));
    }

    for (const auto& global_pair : global_hash_ring_map) {
      logger->info("Hash ring for tier {} is size {}.",
                   std::to_string(global_pair.first),
                   std::to_string(global_pair.second.size()));
    }
  }
}
