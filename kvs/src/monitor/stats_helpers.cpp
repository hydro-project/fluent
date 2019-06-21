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
    map<TierId, GlobalHashRing>& global_hash_rings,
    map<TierId, LocalHashRing>& local_hash_rings, SocketCache& pushers,
    MonitoringThread& mt, zmq::socket_t& response_puller, logger log,
    unsigned& rid, map<Key, map<Address, unsigned>>& key_access_frequency,
    map<Key, map<Address, unsigned>>& hot_key_access_frequency,
    map<Key, map<Address, unsigned>>& cold_key_access_frequency,
    map<Key, unsigned>& key_size, StorageStats& memory_storage,
    StorageStats& ebs_storage, OccupancyStats& memory_occupancy,
    OccupancyStats& ebs_occupancy, AccessStats& memory_accesses,
    AccessStats& ebs_accesses) {
  map<Address, KeyRequest> addr_request_map;

  for (int tier_id = 0; tier_id < global_hash_rings.size(); tier_id++) {
    GlobalHashRing hash_ring = global_hash_rings[tier_id];

    for (const ServerThread& st : hash_ring.get_unique_servers()) {
      for (unsigned i = 0; i < kTierMetadata[tier_id].thread_number_; i++) {
        Key key = get_metadata_key(st, tier_id, i, MetadataType::server_stats);
        prepare_metadata_get_request(key, global_hash_rings[kMemoryTierId],
                                     local_hash_rings[kMemoryTierId],
                                     addr_request_map,
                                     mt.response_connect_address(), rid);

        key = get_metadata_key(st, tier_id, i, MetadataType::key_access_hot);
        prepare_metadata_get_request(key, global_hash_rings[kMemoryTierId],
                                     local_hash_rings[kMemoryTierId],
                                     addr_request_map,
                                     mt.response_connect_address(), rid);

        key = get_metadata_key(st, tier_id, i, MetadataType::key_access_cold);
        prepare_metadata_get_request(key, global_hash_rings[kMemoryTierId],
                                     local_hash_rings[kMemoryTierId],
                                     addr_request_map,
                                     mt.response_connect_address(), rid);

        key = get_metadata_key(st, tier_id, i, MetadataType::key_size);
        prepare_metadata_get_request(key, global_hash_rings[kMemoryTierId],
                                     local_hash_rings[kMemoryTierId],
                                     addr_request_map,
                                     mt.response_connect_address(), rid);
      }
    }
  }

  for (const auto& addr_request_pair : addr_request_map) {
    bool succeed;
    auto res = make_request<KeyRequest, KeyResponse>(
        addr_request_pair.second, pushers[addr_request_pair.first],
        response_puller, succeed);

    if (succeed) {
      for (const KeyTuple& tuple : res.tuples()) {
        if (tuple.error() == 0) {
          vector<string> tokens = split_metadata_key(tuple.key());

          string metadata_type = tokens[1];
          Address ip_pair = tokens[2] + "/" + tokens[3];
          unsigned tid = stoi(tokens[4]);
          unsigned tier_id = stoi(tokens[5]);

          LWWValue lww_value;
          lww_value.ParseFromString(tuple.payload());

          if (metadata_type == "stats") {
            // deserialize the value
            ServerThreadStatistics stat;
            stat.ParseFromString(lww_value.value());

            if (tier_id == 1) {
              memory_storage[ip_pair][tid] = stat.storage_consumption();
              memory_occupancy[ip_pair][tid] =
                  std::pair<double, unsigned>(stat.occupancy(), stat.epoch());
              memory_accesses[ip_pair][tid] = stat.access_count();
            } else {
              ebs_storage[ip_pair][tid] = stat.storage_consumption();
              ebs_occupancy[ip_pair][tid] =
                  std::pair<double, unsigned>(stat.occupancy(), stat.epoch());
              ebs_accesses[ip_pair][tid] = stat.access_count();
            }
          } else if (metadata_type == "access") {
            // deserialized the value
            KeyAccessData access;
            access.ParseFromString(lww_value.value());

            for (const auto& key_count : access.keys()) {
              Key key = key_count.key();
              key_access_frequency[key][ip_pair + ":" + std::to_string(tid)] =
                  key_count.access_count();
            }
          } else if (metadata_type == "hot_access") {
            // deserialized the value
            KeyAccessData access;
            access.ParseFromString(lww_value.value());

            for (const auto& key_count : access.keys()) {
              Key key = key_count.key();
              hot_key_access_frequency[key]
                                      [ip_pair + ":" + std::to_string(tid)] =
                                          key_count.access_count();
            }
          } else if (metadata_type == "cold_access") {
            // deserialized the value
            KeyAccessData access;
            access.ParseFromString(lww_value.value());

            for (const auto& key_count : access.keys()) {
              Key key = key_count.key();
              cold_key_access_frequency[key]
                                       [ip_pair + ":" + std::to_string(tid)] =
                                           key_count.access_count();
            }
          } else if (metadata_type == "size") {
            // deserialized the size
            KeySizeData key_size_msg;
            key_size_msg.ParseFromString(lww_value.value());

            for (const auto& key_size_tuple : key_size_msg.key_sizes()) {
              key_size[key_size_tuple.key()] = key_size_tuple.size();
            }
          }
        } else if (tuple.error() == 1) {
          log->error("Key {} doesn't exist.", tuple.key());
        } else {
          // The hash ring should never be inconsistent.
          log->error("Hash ring is inconsistent for key {}.", tuple.key());
        }
      }
    } else {
      log->error("Request timed out.");
      continue;
    }
  }
}

void compute_summary_stats(
    map<Key, map<Address, unsigned>>& key_access_frequency,
    map<Key, map<Address, unsigned>>& hot_key_access_frequency,
    map<Key, map<Address, unsigned>>& cold_key_access_frequency,
    StorageStats& memory_storage, StorageStats& ebs_storage,
    OccupancyStats& memory_occupancy, OccupancyStats& ebs_occupancy,
    AccessStats& memory_accesses, AccessStats& ebs_accesses,
    map<Key, unsigned>& key_access_summary,
    map<Key, unsigned>& hot_key_access_summary,
    map<Key, unsigned>& cold_key_access_summary, SummaryStats& ss, logger log,
    unsigned& server_monitoring_epoch) {
  // compute key access summary
  unsigned cnt = 0;
  double mean = 0;
  double ms = 0;

  for (const auto& key_access_pair : hot_key_access_frequency) {
    Key key = key_access_pair.first;
    unsigned access_count = 0;

    for (const auto& per_machine_pair : key_access_pair.second) {
      access_count += per_machine_pair.second;
    }

    hot_key_access_summary[key] = access_count;

    if (access_count > 0) {
      cnt += 1;

      double delta = access_count - mean;
      mean += (double)delta / cnt;

      double delta2 = access_count - mean;
      ms += delta * delta2;
    }
  }

  ss.hot_key_access_mean = mean;
  ss.hot_key_access_std = sqrt((double)ms / cnt);

  log->info("Hot Access: mean={}, std={}", ss.hot_key_access_mean,
            ss.hot_key_access_std);

  cnt = 0;
  mean = 0;
  ms = 0;

  for (const auto& key_access_pair : cold_key_access_frequency) {
    Key key = key_access_pair.first;
    unsigned access_count = 0;

    for (const auto& per_machine_pair : key_access_pair.second) {
      access_count += per_machine_pair.second;
    }

    cold_key_access_summary[key] = access_count;

    if (access_count > 0) {
      cnt += 1;

      double delta = access_count - mean;
      mean += (double)delta / cnt;

      double delta2 = access_count - mean;
      ms += delta * delta2;
    }
  }

  ss.cold_key_access_mean = mean;
  ss.cold_key_access_std = sqrt((double)ms / cnt);

  log->info("Cold Access: mean={}, std={}", ss.cold_key_access_mean,
            ss.cold_key_access_std);

  // compute tier access summary
  for (const auto& accesses : memory_accesses) {
    for (const auto& thread_access : accesses.second) {
      ss.total_memory_access += thread_access.second;
    }
  }

  for (const auto& access : ebs_accesses) {
    for (const auto& thread_access : access.second) {
      ss.total_ebs_access += thread_access.second;
    }
  }

  log->info("Total accesses: memory={}, ebs={}", ss.total_memory_access,
            ss.total_ebs_access);

  // compute storage consumption related statistics
  unsigned m_count = 0;
  unsigned e_count = 0;

  for (const auto& memory_storage : memory_storage) {
    unsigned total_thread_consumption = 0;

    for (const auto& thread_storage : memory_storage.second) {
      ss.total_memory_consumption += thread_storage.second;
      total_thread_consumption += thread_storage.second;
    }

    double percentage = (double)total_thread_consumption /
                        (double)kTierMetadata[kMemoryTierId].node_capacity_;
    log->info("Memory node {} storage consumption is {}.", memory_storage.first,
              percentage);

    if (percentage > ss.max_memory_consumption_percentage) {
      ss.max_memory_consumption_percentage = percentage;
    }

    m_count += 1;
  }

  for (const auto& ebs_storage : ebs_storage) {
    unsigned total_thread_consumption = 0;

    for (const auto& thread_storage : ebs_storage.second) {
      ss.total_ebs_consumption += thread_storage.second;
      total_thread_consumption += thread_storage.second;
    }

    double percentage = (double)total_thread_consumption /
                        (double)kTierMetadata[kEbsTierId].node_capacity_;
    log->info("EBS node {} storage consumption is {}.", ebs_storage.first,
              percentage);

    if (percentage > ss.max_ebs_consumption_percentage) {
      ss.max_ebs_consumption_percentage = percentage;
    }
    e_count += 1;
  }

  if (m_count != 0) {
    ss.avg_memory_consumption_percentage =
        (double)ss.total_memory_consumption /
        ((double)m_count * kTierMetadata[kMemoryTierId].node_capacity_);
    log->info("Average memory node consumption is {}.",
              ss.avg_memory_consumption_percentage);
    log->info("Max memory node consumption is {}.",
              ss.max_memory_consumption_percentage);
  }

  if (e_count != 0) {
    ss.avg_ebs_consumption_percentage =
        (double)ss.total_ebs_consumption /
        ((double)e_count * kTierMetadata[kEbsTierId].node_capacity_);
    log->info("Average EBS node consumption is {}.",
              ss.avg_ebs_consumption_percentage);
    log->info("Max EBS node consumption is {}.",
              ss.max_ebs_consumption_percentage);
  }

  ss.required_memory_node = ceil(ss.total_memory_consumption /
                                 (kMaxMemoryNodeConsumption *
                                  kTierMetadata[kMemoryTierId].node_capacity_));
  ss.required_ebs_node =
      ceil(ss.total_ebs_consumption /
           (kMaxEbsNodeConsumption * kTierMetadata[kEbsTierId].node_capacity_));

  log->info("The system requires {} new memory nodes.",
            ss.required_memory_node);
  log->info("The system requires {} new EBS nodes.", ss.required_ebs_node);

  // compute occupancy related statistics
  double sum_memory_occupancy = 0.0;

  unsigned count = 0;

  for (const auto& memory_occ : memory_occupancy) {
    double sum_thread_occupancy = 0.0;
    unsigned thread_count = 0;

    for (const auto& thread_occ : memory_occ.second) {
      log->info(
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
      vector<string> ips;
      split(memory_occ.first, '/', ips);
      ss.min_occupancy_memory_public_ip = ips[0];
      ss.min_occupancy_memory_private_ip = ips[1];
    }

    count += 1;
  }

  ss.avg_memory_occupancy = sum_memory_occupancy / count;
  log->info("Max memory node occupancy is {}.",
            std::to_string(ss.max_memory_occupancy));
  log->info("Min memory node occupancy is {}.",
            std::to_string(ss.min_memory_occupancy));
  log->info("Average memory node occupancy is {}.",
            std::to_string(ss.avg_memory_occupancy));

  double sum_ebs_occupancy = 0.0;

  count = 0;

  for (const auto& ebs_occ : ebs_occupancy) {
    double sum_thread_occupancy = 0.0;
    unsigned thread_count = 0;

    for (const auto& thread_occ : ebs_occ.second) {
      log->info(
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
  log->info("Max EBS node occupancy is {}.",
            std::to_string(ss.max_ebs_occupancy));
  log->info("Min EBS node occupancy is {}.",
            std::to_string(ss.min_ebs_occupancy));
  log->info("Average EBS node occupancy is {}.",
            std::to_string(ss.avg_ebs_occupancy));
}

void collect_external_stats(map<string, double>& user_latency,
                            map<string, double>& user_throughput,
                            SummaryStats& ss, logger log) {
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

  log->info("Average latency is {}.", ss.avg_latency);

  // gather throughput info
  if (user_throughput.size() > 0) {
    // compute latency from users
    for (const auto& thruput_pair : user_throughput) {
      ss.total_throughput += thruput_pair.second;
    }
  }

  log->info("Total throughput is {}.", ss.total_throughput);
}
