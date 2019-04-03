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

void movement_policy(logger log, map<TierId, GlobalHashRing>& global_hash_rings,
                     map<TierId, LocalHashRing>& local_hash_rings,
                     TimePoint& grace_start, SummaryStats& ss,
                     unsigned& memory_node_number, unsigned& ebs_node_number,
                     unsigned& adding_memory_node, unsigned& adding_ebs_node,
                     Address management_ip,
                     map<Key, KeyReplication>& key_replication_map,
                     map<Key, unsigned>& key_access_summary,
                     map<Key, unsigned>& hot_key_access_summary,
                     map<Key, unsigned>& cold_key_access_summary,
                     map<Key, unsigned>& key_size, MonitoringThread& mt,
                     SocketCache& pushers, zmq::socket_t& response_puller,
                     vector<Address>& routing_ips, unsigned& rid) {
  // promote hot keys to memory tier
  map<Key, KeyReplication> requests;
  unsigned total_rep_to_change = 0;
  unsigned long long required_storage = 0;
  unsigned free_storage =
      (kMaxMemoryNodeConsumption * kTierMetadata[kMemoryTierId].node_capacity_ *
           memory_node_number -
       ss.total_memory_consumption);
  bool overflow = false;

  for (const auto& key_access_pair : hot_key_access_summary) {
    Key key = key_access_pair.first;
    unsigned access_count = key_access_pair.second;

    if (!is_metadata(key) && access_count > kKeyPromotionThreshold &&
        key_replication_map[key].global_replication_[kMemoryTierId] == 0 &&
        key_size.find(key) != key_size.end()) {
      required_storage += key_size[key];
      if (required_storage > free_storage) {
        overflow = true;
      } else {
        total_rep_to_change += 1;
        requests[key] = create_new_replication_vector(
            key_replication_map[key].global_replication_[kMemoryTierId] + 1,
            key_replication_map[key].global_replication_[kEbsTierId] - 1,
            key_replication_map[key].local_replication_[kMemoryTierId],
            key_replication_map[key].local_replication_[kEbsTierId]);
      }
    }
  }

  change_replication_factor(requests, global_hash_rings, local_hash_rings,
                            routing_ips, key_replication_map, pushers, mt,
                            response_puller, log, rid);
  log->info("Promoting {} keys into memory tier.", total_rep_to_change);
  auto time_elapsed = std::chrono::duration_cast<std::chrono::seconds>(
                          std::chrono::system_clock::now() - grace_start)
                          .count();

  if (overflow && adding_memory_node == 0 && time_elapsed > kGracePeriod) {
    unsigned total_memory_node_needed =
        ceil((ss.total_memory_consumption + required_storage) /
             (kMaxMemoryNodeConsumption *
              kTierMetadata[kMemoryTierId].node_capacity_));

    if (total_memory_node_needed > memory_node_number) {
      unsigned node_to_add = (total_memory_node_needed - memory_node_number);
      add_node(log, "memory", node_to_add, adding_memory_node, pushers,
               management_ip);
    }
  }

  requests.clear();
  total_rep_to_change = 0;
  required_storage = 0;

  // demote cold keys to ebs tier
  free_storage =
      (kMaxEbsNodeConsumption * kTierMetadata[kEbsTierId].node_capacity_ *
           ebs_node_number -
       ss.total_ebs_consumption);
  overflow = false;

  for (const auto& key_access_pair : cold_key_access_summary) {
    Key key = key_access_pair.first;
    unsigned access_count = key_access_pair.second;

    if (!is_metadata(key) && access_count < kKeyDemotionThreshold &&
        key_replication_map[key].global_replication_[kMemoryTierId] > 0 &&
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

  change_replication_factor(requests, global_hash_rings, local_hash_rings,
                            routing_ips, key_replication_map, pushers, mt,
                            response_puller, log, rid);
  log->info("Demoting {} keys into EBS tier.", total_rep_to_change);
  if (overflow && adding_ebs_node == 0 && time_elapsed > kGracePeriod) {
    unsigned total_ebs_node_needed = ceil(
        (ss.total_ebs_consumption + required_storage) /
        (kMaxEbsNodeConsumption * kTierMetadata[kEbsTierId].node_capacity_));

    if (total_ebs_node_needed > ebs_node_number) {
      unsigned node_to_add = (total_ebs_node_needed - ebs_node_number);
      add_node(log, "ebs", node_to_add, adding_ebs_node, pushers,
               management_ip);
    }
  }

  requests.clear();
  total_rep_to_change = 0;

  // reduce the replication factor of some keys that are not so hot anymore
  KeyReplication minimum_rep =
      create_new_replication_vector(1, kMinimumReplicaNumber - 1, 1, 1);
  for (const auto& key_access_pair : cold_key_access_summary) {
    Key key = key_access_pair.first;
    unsigned access_count = key_access_pair.second;
    if (!is_metadata(key) && !(key_replication_map[key] == minimum_rep)) {
      log->info("Key {} accessed {} times (threshold is {}).", key,
                access_count, ss.cold_key_access_mean + ss.cold_key_access_std);
      requests[key] =
          create_new_replication_vector(1, kMinimumReplicaNumber - 1, 1, 1);
      log->info("Dereplication for key {}. M: {}->{}. E: {}->{}", key,
                key_replication_map[key].global_replication_[kMemoryTierId],
                requests[key].global_replication_[kMemoryTierId],
                key_replication_map[key].global_replication_[kEbsTierId],
                requests[key].global_replication_[kEbsTierId]);
    }
  }

  change_replication_factor(requests, global_hash_rings, local_hash_rings,
                            routing_ips, key_replication_map, pushers, mt,
                            response_puller, log, rid);
  requests.clear();
}
