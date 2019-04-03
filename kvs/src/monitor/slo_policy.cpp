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

void slo_policy(logger log, map<TierId, GlobalHashRing>& global_hash_rings,
                map<TierId, LocalHashRing>& local_hash_rings,
                TimePoint& grace_start, SummaryStats& ss,
                unsigned& memory_node_number, unsigned& adding_memory_node,
                bool& removing_memory_node, Address management_ip,
                map<Key, KeyReplication>& key_replication_map,
                map<Key, unsigned>& key_access_summary,
                map<Key, unsigned>& hot_key_access_summary,
                map<Key, unsigned>& cold_key_access_summary, MonitoringThread& mt,
                map<Address, unsigned>& departing_node_map,
                SocketCache& pushers, zmq::socket_t& response_puller,
                vector<Address>& routing_ips, unsigned& rid,
                map<Key, std::pair<double, unsigned>>& latency_miss_ratio_map) {
  // check latency to trigger elasticity or selective replication
  map<Key, KeyReplication> requests;
  if (ss.avg_latency > kSloWorst && adding_memory_node == 0) {
    log->info("Observed latency ({}) violates SLO({}).", ss.avg_latency,
              kSloWorst);

    // figure out if we should do hot key replication or add nodes
    if (ss.min_memory_occupancy > 0.15) {
      unsigned node_to_add =
          ceil((ss.avg_latency / kSloWorst - 1) * memory_node_number);

      // trigger elasticity
      auto time_elapsed = std::chrono::duration_cast<std::chrono::seconds>(
                              std::chrono::system_clock::now() - grace_start)
                              .count();
      if (time_elapsed > kGracePeriod) {
        add_node(log, "memory", node_to_add, adding_memory_node, pushers,
                 management_ip);
      }
    } else {  // hot key replication
      // find hot keys
      log->info("Classifying hot keys...");
      for (const auto& key_access_pair : hot_key_access_summary) {
        Key key = key_access_pair.first;
        unsigned access_count = key_access_pair.second;

        if (!is_metadata(key) &&
            access_count > ss.hot_key_access_mean &&
            latency_miss_ratio_map.find(key) != latency_miss_ratio_map.end()) {
          log->info("Key {} accessed {} times (threshold is {}).", key,
                    access_count, ss.hot_key_access_mean);
          unsigned target_rep_factor =
              key_replication_map[key].global_replication_[kMemoryTierId] *
              latency_miss_ratio_map[key].first;

          if (target_rep_factor ==
              key_replication_map[key].global_replication_[kMemoryTierId]) {
            target_rep_factor += 1;
          }

          unsigned current_mem_rep =
              key_replication_map[key].global_replication_[kMemoryTierId];
          if (target_rep_factor > current_mem_rep &&
              current_mem_rep < memory_node_number) {
            unsigned new_mem_rep =
                std::min(memory_node_number, target_rep_factor);
            unsigned new_ebs_rep =
                std::max(kMinimumReplicaNumber - new_mem_rep, (unsigned)0);
            requests[key] = create_new_replication_vector(
                new_mem_rep, new_ebs_rep,
                key_replication_map[key].local_replication_[kMemoryTierId],
                key_replication_map[key].local_replication_[kEbsTierId]);
            log->info(
                "Global hot key replication for key {}. M: {}->{}.", key,
                key_replication_map[key].global_replication_[kMemoryTierId],
                requests[key].global_replication_[kMemoryTierId]);
          } else {
            if (kMemoryThreadCount >
                key_replication_map[key].local_replication_[kMemoryTierId]) {
              requests[key] = create_new_replication_vector(
                  key_replication_map[key].global_replication_[kMemoryTierId],
                  key_replication_map[key].global_replication_[kEbsTierId],
                  kMemoryThreadCount,
                  key_replication_map[key].local_replication_[kEbsTierId]);
              log->info(
                  "Local hot key replication for key {}. T: {}->{}.", key,
                  key_replication_map[key].local_replication_[kMemoryTierId],
                  requests[key].local_replication_[kMemoryTierId]);
            }
          }
        }
      }

      change_replication_factor(requests, global_hash_rings, local_hash_rings,
                                routing_ips, key_replication_map, pushers, mt,
                                response_puller, log, rid);
    }
  } else if (ss.min_memory_occupancy < 0.05 && !removing_memory_node &&
             memory_node_number > std::max(ss.required_memory_node,
                                           (unsigned)kMinMemoryTierSize)) {
    log->info("Node {}/{} is severely underutilized.",
              ss.min_occupancy_memory_public_ip,
              ss.min_occupancy_memory_private_ip);
    auto time_elapsed = std::chrono::duration_cast<std::chrono::seconds>(
                            std::chrono::system_clock::now() - grace_start)
                            .count();

    if (time_elapsed > kGracePeriod) {
      // before sending remove command, first adjust relevant key's replication
      // factor
      for (const auto& key_access_pair : hot_key_access_summary) {
        Key key = key_access_pair.first;

        if (!is_metadata(key) &&
            metadata_map[key].global_replication_[kMemoryTierId] ==
                (global_hash_rings[kMemoryTierId].size() / kVirtualThreadNum)) {
          unsigned new_mem_rep =
              metadata_map[key].global_replication_[kMemoryTierId] - 1;
          unsigned new_ebs_rep =
              std::max(kMinimumReplicaNumber - new_mem_rep, (unsigned)0);
          requests[key] = create_new_replication_vector(
              new_mem_rep, new_ebs_rep,
              metadata_map[key].local_replication_[kMemoryTierId],
              metadata_map[key].local_replication_[kEbsTierId]);
          log->info("Dereplication for key {}. M: {}->{}. E: {}->{}", key,
                    metadata_map[key].global_replication_[kMemoryTierId],
                    requests[key].global_replication_[kMemoryTierId],
                    metadata_map[key].global_replication_[kEbsTierId],
                    requests[key].global_replication_[kEbsTierId]);
        }
      }

      for (const auto& key_access_pair : cold_key_access_summary) {
        Key key = key_access_pair.first;

        if (!is_metadata(key) &&
            key_replication_map[key].global_replication_[kMemoryTierId] ==
                (global_hash_rings[kMemoryTierId].size() / kVirtualThreadNum)) {
          unsigned new_mem_rep =
              key_replication_map[key].global_replication_[kMemoryTierId] - 1;
          unsigned new_ebs_rep =
              std::max(kMinimumReplicaNumber - new_mem_rep, (unsigned)0);
          requests[key] = create_new_replication_vector(
              new_mem_rep, new_ebs_rep,
              key_replication_map[key].local_replication_[kMemoryTierId],
              key_replication_map[key].local_replication_[kEbsTierId]);
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

      ServerThread node = ServerThread(ss.min_occupancy_memory_public_ip,
                                       ss.min_occupancy_memory_private_ip, 0);
      remove_node(log, node, "memory", removing_memory_node, pushers,
                  departing_node_map, mt);
    }
  }
}
