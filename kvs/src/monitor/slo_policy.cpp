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

void slo_policy(
    std::shared_ptr<spdlog::logger> logger,
    std::unordered_map<unsigned, GlobalHashRing>& global_hash_ring_map,
    std::unordered_map<unsigned, LocalHashRing>& local_hash_ring_map,
    std::chrono::time_point<std::chrono::system_clock>& grace_start,
    SummaryStats& ss, unsigned& memory_node_number,
    unsigned& adding_memory_node, bool& removing_memory_node,
    Address management_address, std::unordered_map<Key, KeyInfo>& placement,
    std::unordered_map<Key, unsigned>& key_access_summary, MonitoringThread& mt,
    std::unordered_map<Address, unsigned>& departing_node_map,
    SocketCache& pushers, zmq::socket_t& response_puller,
    std::vector<Address>& routing_address, unsigned& rid,
    std::unordered_map<Key, std::pair<double, unsigned>>&
        latency_miss_ratio_map) {
  // check latency to trigger elasticity or selective replication
  std::unordered_map<Key, KeyInfo> requests;
  if (ss.avg_latency > kSloWorst && adding_memory_node == 0) {
    logger->info("Observed latency ({}) violates SLO({}).", ss.avg_latency,
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
        add_node(logger, "memory", node_to_add, adding_memory_node,
                 management_address);
      }
    } else {  // hot key replication
      // find hot keys
      logger->info("Classifying hot keys...");
      for (const auto& key_access_pair : key_access_summary) {
        Key key = key_access_pair.first;
        unsigned total_access = key_access_pair.second;

        if (!is_metadata(key) &&
            total_access > ss.key_access_mean + ss.key_access_std &&
            latency_miss_ratio_map.find(key) != latency_miss_ratio_map.end()) {
          logger->info("Key {} accessed {} times (threshold is {}).", key,
                       total_access, ss.key_access_mean + ss.key_access_std);
          unsigned target_rep_factor =
              placement[key].global_replication_map_[1] *
              latency_miss_ratio_map[key].first;

          if (target_rep_factor == placement[key].global_replication_map_[1]) {
            target_rep_factor += 1;
          }

          unsigned current_mem_rep = placement[key].global_replication_map_[1];
          if (target_rep_factor > current_mem_rep &&
              current_mem_rep < memory_node_number) {
            unsigned new_mem_rep =
                std::min(memory_node_number, target_rep_factor);
            unsigned new_ebs_rep =
                std::max(kMinimumReplicaNumber - new_mem_rep, (unsigned)0);
            requests[key] = create_new_replication_vector(
                new_mem_rep, new_ebs_rep,
                placement[key].local_replication_map_[1],
                placement[key].local_replication_map_[2]);
            logger->info("Global hot key replication for key {}. M: {}->{}.",
                         key, placement[key].global_replication_map_[1],
                         requests[key].global_replication_map_[1]);
          } else {
            if (kMemoryThreadCount > placement[key].local_replication_map_[1]) {
              requests[key] = create_new_replication_vector(
                  placement[key].global_replication_map_[1],
                  placement[key].global_replication_map_[2], kMemoryThreadCount,
                  placement[key].local_replication_map_[2]);
              logger->info("Local hot key replication for key {}. T: {}->{}.",
                           key, placement[key].local_replication_map_[1],
                           requests[key].local_replication_map_[1]);
            }
          }
        }
      }

      change_replication_factor(requests, global_hash_ring_map,
                                local_hash_ring_map, routing_address, placement,
                                pushers, mt, response_puller, logger, rid);
    }
  } else if (ss.min_memory_occupancy < 0.05 && !removing_memory_node &&
             memory_node_number > std::max(ss.required_memory_node,
                                           (unsigned)kMinMemoryTierSize)) {
    logger->info("Node {} is severely underutilized.",
                 ss.min_occupancy_memory_ip);
    auto time_elapsed = std::chrono::duration_cast<std::chrono::seconds>(
                            std::chrono::system_clock::now() - grace_start)
                            .count();

    if (time_elapsed > kGracePeriod) {
      // before sending remove command, first adjust relevant key's replication
      // factor
      for (const auto& key_access_pair : key_access_summary) {
        Key key = key_access_pair.first;

        if (!is_metadata(key) &&
            placement[key].global_replication_map_[1] ==
                (global_hash_ring_map[1].size() / kVirtualThreadNum)) {
          unsigned new_mem_rep = placement[key].global_replication_map_[1] - 1;
          unsigned new_ebs_rep =
              std::max(kMinimumReplicaNumber - new_mem_rep, (unsigned)0);
          requests[key] = create_new_replication_vector(
              new_mem_rep, new_ebs_rep,
              placement[key].local_replication_map_[1],
              placement[key].local_replication_map_[2]);
          logger->info("Dereplication for key {}. M: {}->{}. E: {}->{}", key,
                       placement[key].global_replication_map_[1],
                       requests[key].global_replication_map_[1],
                       placement[key].global_replication_map_[2],
                       requests[key].global_replication_map_[2]);
        }
      }

      change_replication_factor(requests, global_hash_ring_map,
                                local_hash_ring_map, routing_address, placement,
                                pushers, mt, response_puller, logger, rid);

      ServerThread node = ServerThread(ss.min_occupancy_memory_ip, 0);
      remove_node(logger, node, "memory", removing_memory_node, pushers,
                  departing_node_map, mt);
    }
  }
}
