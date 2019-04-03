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
    logger log, string& serialized,
    map<TierId, GlobalHashRing>& global_hash_rings, unsigned& new_memory_count,
    unsigned& new_ebs_count, TimePoint& grace_start,
    vector<Address>& routing_ips, StorageStats& memory_storage,
    StorageStats& ebs_storage, OccupancyStats& memory_occupancy,
    OccupancyStats& ebs_occupancy,
    map<Key, map<Address, unsigned>>& key_access_frequency,
    map<Key, map<Address, unsigned>>& hot_key_access_frequency,
    map<Key, map<Address, unsigned>>& cold_key_access_frequency) {
  vector<string> v;

  split(serialized, ':', v);
  string type = v[0];
  unsigned tier = stoi(v[1]);
  Address new_server_public_ip = v[2];
  Address new_server_private_ip = v[3];

  if (type == "join") {
    log->info("Received join from server {}/{} in tier {}.",
              new_server_public_ip, new_server_private_ip,
              std::to_string(tier));
    if (tier == kMemoryTierId) {
      global_hash_rings[tier].insert(new_server_public_ip,
                                     new_server_private_ip, 0, 0);

      if (new_memory_count > 0) {
        new_memory_count -= 1;
      }

      // reset grace period timer
      grace_start = std::chrono::system_clock::now();
    } else if (tier == kEbsTierId) {
      global_hash_rings[tier].insert(new_server_public_ip,
                                     new_server_private_ip, 0, 0);

      if (new_ebs_count > 0) {
        new_ebs_count -= 1;
      }

      // reset grace period timer
      grace_start = std::chrono::system_clock::now();
    } else if (tier == kRoutingTierId) {
      routing_ips.push_back(new_server_public_ip);
    } else {
      log->error("Invalid tier: {}.", std::to_string(tier));
    }

    for (const auto& pair : global_hash_rings) {
      log->info("Hash ring for tier {} is size {}.", pair.first,
                pair.second.size());
    }
  } else if (type == "depart") {
    log->info("Received depart from server {}/{}.", new_server_public_ip,
              new_server_private_ip);

    // update hash ring
    global_hash_rings[tier].remove(new_server_public_ip, new_server_private_ip,
                                   0);
    if (tier == kMemoryTierId) {
      memory_storage.erase(new_server_private_ip);
      memory_occupancy.erase(new_server_private_ip);

      // NOTE: No const here because we are calling erase
      for (auto& key_access_pair : hot_key_access_frequency) {
        for (unsigned i = 0; i < kMemoryThreadCount; i++) {
          key_access_pair.second.erase(new_server_private_ip + ":" +
                                       std::to_string(i));
        }
      }
      for (auto& key_access_pair : cold_key_access_frequency) {
        for (unsigned i = 0; i < kMemoryThreadCount; i++) {
          key_access_pair.second.erase(new_server_private_ip + ":" +
                                       std::to_string(i));
        }
      }
    } else if (tier == kEbsTierId) {
      ebs_storage.erase(new_server_private_ip);
      ebs_occupancy.erase(new_server_private_ip);

      // NOTE: No const here because we are calling erase
      for (auto& key_access_pair : hot_key_access_frequency) {
        for (unsigned i = 0; i < kEbsThreadCount; i++) {
          key_access_pair.second.erase(new_server_private_ip + ":" +
                                       std::to_string(i));
        }
      }
      for (auto& key_access_pair : cold_key_access_frequency) {
        for (unsigned i = 0; i < kEbsThreadCount; i++) {
          key_access_pair.second.erase(new_server_private_ip + ":" +
                                       std::to_string(i));
        }
      }
    } else {
      log->error("Invalid tier: {}.", std::to_string(tier));
    }

    for (const auto& pair : global_hash_rings) {
      log->info("Hash ring for tier {} is size {}.", pair.first,
                pair.second.size());
    }
  }
}
