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
#include "requests.hpp"

void collect_internal_stats(
    std::unordered_map<unsigned, GlobalHashRing>& global_hash_ring_map,
    std::unordered_map<unsigned, LocalHashRing>& local_hash_ring_map,
    SocketCache& pushers, MonitoringThread& mt, zmq::socket_t& response_puller,
    std::shared_ptr<spdlog::logger> logger, unsigned& rid,
    std::unordered_map<Key, std::unordered_map<Address, unsigned>>&
        key_access_frequency,
    std::unordered_map<Key, unsigned>& key_size,
    StorageStat& memory_tier_storage, StorageStat& ebs_tier_storage,
    OccupancyStat& memory_tier_occupancy, OccupancyStat& ebs_tier_occupancy,
    AccessStat& memory_tier_access, AccessStat& ebs_tier_access) {
  std::unordered_map<Address, KeyRequest> addr_request_map;

  for (const auto& global_pair : global_hash_ring_map) {
    unsigned tier_id = global_pair.first;
    auto hash_ring = global_pair.second;

    for (const ServerThread& st : hash_ring.get_unique_servers()) {
      for (unsigned i = 0; i < kTierDataMap[tier_id].thread_number_; i++) {
        Key key = get_metadata_key(st, tier_id, i, MetadataType::server_stats);
        prepare_metadata_get_request(key, global_hash_ring_map[1],
                                     local_hash_ring_map[1], addr_request_map,
                                     mt, rid);

        key = get_metadata_key(st, tier_id, i, MetadataType::key_access);
        prepare_metadata_get_request(key, global_hash_ring_map[1],
                                     local_hash_ring_map[1], addr_request_map,
                                     mt, rid);

        key = get_metadata_key(st, tier_id, i, MetadataType::key_size);
        prepare_metadata_get_request(key, global_hash_ring_map[1],
                                     local_hash_ring_map[1], addr_request_map,
                                     mt, rid);
      }
    }
  }

  for (const auto& addr_request_pair : addr_request_map) {
    bool succeed;
    auto res = send_request<KeyRequest, KeyResponse>(
        addr_request_pair.second, pushers[addr_request_pair.first],
        response_puller, succeed);

    if (succeed) {
      for (const KeyTuple& tuple : res.tuples()) {
        if (tuple.error() == 0) {
          std::vector<std::string> tokens = split_metadata_key(tuple.key());

          Address ip = tokens[1];
          unsigned tid = stoi(tokens[2]);
          unsigned tier_id = stoi(tokens[3]);
          std::string metadata_type = tokens[4];

          if (metadata_type == "stat") {
            // deserialized the value
            ServerThreadStatistics stat;
            stat.ParseFromString(tuple.value());

            if (tier_id == 1) {
              memory_tier_storage[ip][tid] = stat.storage_consumption();
              memory_tier_occupancy[ip][tid] =
                  std::pair<double, unsigned>(stat.occupancy(), stat.epoch());
              memory_tier_access[ip][tid] = stat.total_accesses();
            } else {
              ebs_tier_storage[ip][tid] = stat.storage_consumption();
              ebs_tier_occupancy[ip][tid] =
                  std::pair<double, unsigned>(stat.occupancy(), stat.epoch());
              ebs_tier_access[ip][tid] = stat.total_accesses();
            }
          } else if (metadata_type == "access") {
            // deserialized the value
            KeyAccessData access;
            access.ParseFromString(tuple.value());

            for (const auto& key_count : access.keys()) {
              Key key = key_count.key();
              key_access_frequency[key][ip + ":" + std::to_string(tid)] =
                  key_count.access_count();
            }
          } else if (metadata_type == "size") {
            // deserialized the size
            KeySizeData key_size_msg;
            key_size_msg.ParseFromString(tuple.value());

            for (const auto& key_size_tuple : key_size_msg.key_sizes()) {
              key_size[key_size_tuple.key()] = key_size_tuple.size();
            }
          }
        } else if (tuple.error() == 1) {
          logger->error("Key {} doesn't exist.", tuple.key());
        } else {
          // The hash ring should never be inconsistent.
          logger->error("Hash ring is inconsistent for key {}.", tuple.key());
        }
      }
    } else {
      logger->error("Request timed out.");
      continue;
    }
  }
}

void compute_summary_stats(
    std::unordered_map<Key, std::unordered_map<Address, unsigned>>&
        key_access_frequency,
    StorageStat& memory_tier_storage, StorageStat& ebs_tier_storage,
    OccupancyStat& memory_tier_occupancy, OccupancyStat& ebs_tier_occupancy,
    AccessStat& memory_tier_access, AccessStat& ebs_tier_access,
    std::unordered_map<Key, unsigned>& key_access_summary, SummaryStats& ss,
    std::shared_ptr<spdlog::logger> logger, unsigned& server_monitoring_epoch) {
  // compute key access summary
  unsigned cnt = 0;
  double mean = 0;
  double ms = 0;

  for (const auto& key_access_pair : key_access_frequency) {
    Key key = key_access_pair.first;
    unsigned total_access = 0;

    for (const auto& per_machine_pair : key_access_pair.second) {
      total_access += per_machine_pair.second;
    }

    key_access_summary[key] = total_access;

    if (total_access > 0) {
      cnt += 1;

      double delta = total_access - mean;
      mean += (double)delta / cnt;

      double delta2 = total_access - mean;
      ms += delta * delta2;
    }
  }

  ss.key_access_mean = mean;
  ss.key_access_std = sqrt((double)ms / cnt);

  logger->info("Access: mean={}, std={}", ss.key_access_mean,
               ss.key_access_std);

  // compute tier access summary
  for (const auto& memory_access : memory_tier_access) {
    for (const auto& thread_access : memory_access.second) {
      ss.total_memory_access += thread_access.second;
    }
  }

  for (const auto& ebs_access : ebs_tier_access) {
    for (const auto& thread_access : ebs_access.second) {
      ss.total_ebs_access += thread_access.second;
    }
  }

  logger->info("Total accesses: memory={}, ebs={}", ss.total_memory_access,
               ss.total_ebs_access);

  // compute storage consumption related statistics
  unsigned m_count = 0;
  unsigned e_count = 0;

  for (const auto& memory_storage : memory_tier_storage) {
    unsigned total_thread_consumption = 0;

    for (const auto& thread_storage : memory_storage.second) {
      ss.total_memory_consumption += thread_storage.second;
      total_thread_consumption += thread_storage.second;
    }

    double percentage = (double)total_thread_consumption /
                        (double)kTierDataMap[1].node_capacity_;
    logger->info("Memory node {} storage consumption is {}.",
                 memory_storage.first, percentage);

    if (percentage > ss.max_memory_consumption_percentage) {
      ss.max_memory_consumption_percentage = percentage;
    }

    m_count += 1;
  }

  for (const auto& ebs_storage : ebs_tier_storage) {
    unsigned total_thread_consumption = 0;

    for (const auto& thread_storage : ebs_storage.second) {
      ss.total_ebs_consumption += thread_storage.second;
      total_thread_consumption += thread_storage.second;
    }

    double percentage = (double)total_thread_consumption /
                        (double)kTierDataMap[2].node_capacity_;
    logger->info("EBS node {} storage consumption is {}.", ebs_storage.first,
                 percentage);

    if (percentage > ss.max_ebs_consumption_percentage) {
      ss.max_ebs_consumption_percentage = percentage;
    }
    e_count += 1;
  }

  if (m_count != 0) {
    ss.avg_memory_consumption_percentage =
        (double)ss.total_memory_consumption /
        ((double)m_count * kTierDataMap[1].node_capacity_);
    logger->info("Average memory node consumption is {}.",
                 ss.avg_memory_consumption_percentage);
    logger->info("Max memory node consumption is {}.",
                 ss.max_memory_consumption_percentage);
  }

  if (e_count != 0) {
    ss.avg_ebs_consumption_percentage =
        (double)ss.total_ebs_consumption /
        ((double)e_count * kTierDataMap[2].node_capacity_);
    logger->info("Average EBS node consumption is {}.",
                 ss.avg_ebs_consumption_percentage);
    logger->info("Max EBS node consumption is {}.",
                 ss.max_ebs_consumption_percentage);
  }

  ss.required_memory_node =
      ceil(ss.total_memory_consumption /
           (kMaxMemoryNodeConsumption * kTierDataMap[1].node_capacity_));
  ss.required_ebs_node =
      ceil(ss.total_ebs_consumption /
           (kMaxEbsNodeConsumption * kTierDataMap[2].node_capacity_));

  logger->info("The system requires {} new memory nodes.",
               ss.required_memory_node);
  logger->info("The system requires {} new EBS nodes.", ss.required_ebs_node);

  // compute occupancy related statistics
  double sum_memory_occupancy = 0.0;

  unsigned count = 0;

  for (const auto& memory_occ : memory_tier_occupancy) {
    double sum_thread_occupancy = 0.0;
    unsigned thread_count = 0;

    for (const auto& thread_occ : memory_occ.second) {
      logger->info(
          "Memory node {} thread {} occupancy is {} at epoch {} (monitoring "
          "epoch {}).",
          memory_occ.first, thread_occ.first, thread_occ.second.first,
          thread_occ.second.second, server_monitoring_epoch);

      sum_thread_occupancy += thread_occ.second.first;
      thread_count += 1;
    }

    double node_occupancy = sum_thread_occupancy / thread_count;
    sum_memory_occupancy += node_occupancy;

    if (node_occupancy > ss.max_memory_occupancy) {
      ss.max_memory_occupancy = node_occupancy;
    }

    if (node_occupancy < ss.min_memory_occupancy) {
      ss.min_memory_occupancy = node_occupancy;
      ss.min_occupancy_memory_ip = memory_occ.first;
    }

    count += 1;
  }

  ss.avg_memory_occupancy = sum_memory_occupancy / count;
  logger->info("Max memory node occupancy is {}.",
               std::to_string(ss.max_memory_occupancy));
  logger->info("Min memory node occupancy is {}.",
               std::to_string(ss.min_memory_occupancy));
  logger->info("Average memory node occupancy is {}.",
               std::to_string(ss.avg_memory_occupancy));

  double sum_ebs_occupancy = 0.0;

  count = 0;

  for (const auto& ebs_occ : ebs_tier_occupancy) {
    double sum_thread_occupancy = 0.0;
    unsigned thread_count = 0;

    for (const auto& thread_occ : ebs_occ.second) {
      logger->info(
          "EBS node {} thread {} occupancy is {} at epoch {} (monitoring epoch "
          "{}).",
          ebs_occ.first, thread_occ.first, thread_occ.second.first,
          thread_occ.second.second, server_monitoring_epoch);

      sum_thread_occupancy += thread_occ.second.first;
      thread_count += 1;
    }

    double node_occupancy = sum_thread_occupancy / thread_count;
    sum_ebs_occupancy += node_occupancy;

    if (node_occupancy > ss.max_ebs_occupancy) {
      ss.max_ebs_occupancy = node_occupancy;
    }

    if (node_occupancy < ss.min_ebs_occupancy) {
      ss.min_ebs_occupancy = node_occupancy;
    }

    count += 1;
  }

  ss.avg_ebs_occupancy = sum_ebs_occupancy / count;
  logger->info("Max EBS node occupancy is {}.",
               std::to_string(ss.max_ebs_occupancy));
  logger->info("Min EBS node occupancy is {}.",
               std::to_string(ss.min_ebs_occupancy));
  logger->info("Average EBS node occupancy is {}.",
               std::to_string(ss.avg_ebs_occupancy));
}

void collect_external_stats(
    std::unordered_map<std::string, double>& user_latency,
    std::unordered_map<std::string, double>& user_throughput, SummaryStats& ss,
    std::shared_ptr<spdlog::logger> logger) {
  // gather latency info
  if (user_latency.size() > 0) {
    // compute latency from users
    double sum_latency = 0;
    unsigned count = 0;

    for (const auto& latency_pair : user_latency) {
      sum_latency += latency_pair.second;
      count += 1;
    }

    ss.avg_latency = sum_latency / count;
  }

  logger->info("Average latency is {}.", ss.avg_latency);

  // gather throughput info
  if (user_throughput.size() > 0) {
    // compute latency from users
    for (const auto& thruput_pair : user_throughput) {
      ss.total_throughput += thruput_pair.second;
    }
  }

  logger->info("Total throughput is {}.", ss.total_throughput);
}
