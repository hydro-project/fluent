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

#include "monitor/monitoring_utils.hpp"
#include "monitor/policies.hpp"

void movement_policy(
    std::shared_ptr<spdlog::logger> logger,
    std::unordered_map<unsigned, GlobalHashRing>& global_hash_ring_map,
    std::unordered_map<unsigned, LocalHashRing>& local_hash_ring_map,
    std::chrono::time_point<std::chrono::system_clock>& grace_start,
    SummaryStats& ss, unsigned& memory_node_number, unsigned& ebs_node_number,
    unsigned& adding_memory_node, unsigned& adding_ebs_node,
    Address management_address, std::unordered_map<Key, KeyInfo>& placement,
    std::unordered_map<Key, unsigned>& key_access_summary,
    std::unordered_map<Key, unsigned>& key_size, MonitoringThread& mt,
    SocketCache& pushers, zmq::socket_t& response_puller,
    std::vector<Address>& routing_address, unsigned& rid) {
  // promote hot keys to memory tier
  std::unordered_map<Key, KeyInfo> requests;
  unsigned total_rep_to_change = 0;
  unsigned long long required_storage = 0;
  unsigned free_storage =
      (kMaxMemoryNodeConsumption * kTierDataMap[1].node_capacity_ *
           memory_node_number -
       ss.total_memory_consumption);
  bool overflow = false;

  for (const auto& key_access_pair : key_access_summary) {
    Key key = key_access_pair.first;
    unsigned total_access = key_access_pair.second;

    if (!is_metadata(key) && total_access > kKeyPromotionThreshold &&
        placement[key].global_replication_map_[1] == 0 &&
        key_size.find(key) != key_size.end()) {
      required_storage += key_size[key];
      if (required_storage > free_storage) {
        overflow = true;
      } else {
        total_rep_to_change += 1;
        requests[key] = create_new_replication_vector(
            placement[key].global_replication_map_[1] + 1,
            placement[key].global_replication_map_[2] - 1,
            placement[key].local_replication_map_[1],
            placement[key].local_replication_map_[2]);
      }
    }
  }

  change_replication_factor(requests, global_hash_ring_map, local_hash_ring_map,
                            routing_address, placement, pushers, mt,
                            response_puller, logger, rid);
  logger->info("Promoting {} keys into memory tier.", total_rep_to_change);
  auto time_elapsed = std::chrono::duration_cast<std::chrono::seconds>(
                          std::chrono::system_clock::now() - grace_start)
                          .count();

  if (overflow && adding_memory_node == 0 && time_elapsed > kGracePeriod) {
    unsigned total_memory_node_needed =
        ceil((ss.total_memory_consumption + required_storage) /
             (kMaxMemoryNodeConsumption * kTierDataMap[1].node_capacity_));

    if (total_memory_node_needed > memory_node_number) {
      unsigned node_to_add = (total_memory_node_needed - memory_node_number);
      add_node(logger, "memory", node_to_add, adding_memory_node,
               management_address);
    }
  }

  requests.clear();
  total_rep_to_change = 0;
  required_storage = 0;

  // demote cold keys to ebs tier
  free_storage = (kMaxEbsNodeConsumption * kTierDataMap[2].node_capacity_ *
                      ebs_node_number -
                  ss.total_ebs_consumption);
  overflow = false;

  for (const auto& key_access_pair : key_access_summary) {
    Key key = key_access_pair.first;
    unsigned total_access = key_access_pair.second;

    if (!is_metadata(key) && total_access < kKeyDemotionThreshold &&
        placement[key].global_replication_map_[1] > 0 &&
        key_size.find(key) != key_size.end()) {
      required_storage += key_size[key];
      if (required_storage > free_storage) {
        overflow = true;
      } else {
        total_rep_to_change += 1;
        requests[key] =
            create_new_replication_vector(0, kMinimumReplicaNumber, 1, 1);
      }
    }
  }

  change_replication_factor(requests, global_hash_ring_map, local_hash_ring_map,
                            routing_address, placement, pushers, mt,
                            response_puller, logger, rid);
  logger->info("Demoting {} keys into EBS tier.", total_rep_to_change);
  if (overflow && adding_ebs_node == 0 && time_elapsed > kGracePeriod) {
    unsigned total_ebs_node_needed =
        ceil((ss.total_ebs_consumption + required_storage) /
             (kMaxEbsNodeConsumption * kTierDataMap[2].node_capacity_));

    if (total_ebs_node_needed > ebs_node_number) {
      unsigned node_to_add = (total_ebs_node_needed - ebs_node_number);
      add_node(logger, "ebs", node_to_add, adding_ebs_node, management_address);
    }
  }

  requests.clear();
  total_rep_to_change = 0;

  // reduce the replication factor of some keys that are not so hot anymore
  for (const auto& key_access_pair : key_access_summary) {
    Key key = key_access_pair.first;
    unsigned total_access = key_access_pair.second;

    if (!is_metadata(key) && total_access <= ss.key_access_mean) {
      logger->info("Key {} accessed {} times (threshold is {}).", key,
                   total_access, ss.key_access_mean);
      requests[key] =
          create_new_replication_vector(1, kMinimumReplicaNumber - 1, 1, 1);
      logger->info("Dereplication for key {}. M: {}->{}. E: {}->{}", key,
                   placement[key].global_replication_map_[1],
                   requests[key].global_replication_map_[1],
                   placement[key].global_replication_map_[2],
                   requests[key].global_replication_map_[2]);
    }
  }

  change_replication_factor(requests, global_hash_ring_map, local_hash_ring_map,
                            routing_address, placement, pushers, mt,
                            response_puller, logger, rid);
  requests.clear();
}
