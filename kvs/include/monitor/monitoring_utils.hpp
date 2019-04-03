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

#ifndef KVS_INCLUDE_MONITOR_MONITORING_UTILS_HPP_
#define KVS_INCLUDE_MONITOR_MONITORING_UTILS_HPP_

#include "hash_ring.hpp"
#include "metadata.pb.h"
#include "replication.pb.h"
#include "requests.hpp"

// define monitoring threshold (in second)
const unsigned kMonitoringThreshold = 30;

// define the grace period for triggering elasticity action (in second)
const unsigned kGracePeriod = 120;

// the default number of nodes to add concurrently for storage
const unsigned kNodeAdditionBatchSize = 2;

// define capacity for both tiers
const double kMaxMemoryNodeConsumption = 0.6;
const double kMinMemoryNodeConsumption = 0.3;
const double kMaxEbsNodeConsumption = 0.75;
const double kMinEbsNodeConsumption = 0.5;

// define threshold for promotion/demotion
const unsigned kKeyPromotionThreshold = 0;
const unsigned kKeyDemotionThreshold = 1;

// define minimum number of nodes for each tier
const unsigned kMinMemoryTierSize = 1;
const unsigned kMinEbsTierSize = 0;

// value size in KB
const unsigned kValueSize = 256;

struct SummaryStats {
  void clear() {
    key_access_mean = 0;
    key_access_std = 0;
    hot_key_access_mean = 0;
    hot_key_access_std = 0;
    cold_key_access_mean = 0;
    cold_key_access_std = 0;
    total_memory_access = 0;
    total_ebs_access = 0;
    total_memory_consumption = 0;
    total_ebs_consumption = 0;
    max_memory_consumption_percentage = 0;
    max_ebs_consumption_percentage = 0;
    avg_memory_consumption_percentage = 0;
    avg_ebs_consumption_percentage = 0;
    required_memory_node = 0;
    required_ebs_node = 0;
    max_memory_occupancy = 0;
    min_memory_occupancy = 1;
    avg_memory_occupancy = 0;
    max_ebs_occupancy = 0;
    min_ebs_occupancy = 1;
    avg_ebs_occupancy = 0;
    min_occupancy_memory_public_ip = Address();
    min_occupancy_memory_private_ip = Address();
    avg_latency = 0;
    total_throughput = 0;
  }
  SummaryStats() { clear(); }
  double key_access_mean;
  double key_access_std;
  double hot_key_access_mean;
  double hot_key_access_std;
  double cold_key_access_mean;
  double cold_key_access_std;
  unsigned total_memory_access;
  unsigned total_ebs_access;
  unsigned long long total_memory_consumption;
  unsigned long long total_ebs_consumption;
  double max_memory_consumption_percentage;
  double max_ebs_consumption_percentage;
  double avg_memory_consumption_percentage;
  double avg_ebs_consumption_percentage;
  unsigned required_memory_node;
  unsigned required_ebs_node;
  double max_memory_occupancy;
  double min_memory_occupancy;
  double avg_memory_occupancy;
  double max_ebs_occupancy;
  double min_ebs_occupancy;
  double avg_ebs_occupancy;
  Address min_occupancy_memory_public_ip;
  Address min_occupancy_memory_private_ip;
  double avg_latency;
  double total_throughput;
};

void collect_internal_stats(
    map<TierId, GlobalHashRing>& global_hash_rings,
    map<TierId, LocalHashRing>& local_hash_rings, SocketCache& pushers,
    MonitoringThread& mt, zmq::socket_t& response_puller, logger log,
    unsigned& rid, map<Key, map<Address, unsigned>>& key_access_frequency,
    map<Key, map<Address, unsigned>>& hot_key_access_frequency,
    map<Key, map<Address, unsigned>>& cold_key_access_frequency,
    map<Key, unsigned>& key_size, StorageStats& memory_storage,
    StorageStats& ebs_storage, OccupancyStats& memory_occupancy,
    OccupancyStats& ebs_occupancy, AccessStats& memory_access,
    AccessStats& ebs_access);

void compute_summary_stats(
    map<Key, map<Address, unsigned>>& key_access_frequency,
    map<Key, map<Address, unsigned>>& hot_key_access_frequency,
    map<Key, map<Address, unsigned>>& cold_key_access_frequency,
    StorageStats& memory_storage, StorageStats& ebs_storage,
    OccupancyStats& memory_occupancy, OccupancyStats& ebs_occupancy,
    AccessStats& memory_access, AccessStats& ebs_access,
    map<Key, unsigned>& key_access_summary,
    map<Key, unsigned>& hot_key_access_summary,
    map<Key, unsigned>& cold_key_access_summary, SummaryStats& ss, logger log,
    unsigned& server_monitoring_epoch);

void collect_external_stats(map<string, double>& user_latency,
                            map<string, double>& user_throughput,
                            SummaryStats& ss, logger log);

KeyReplication create_new_replication_vector(unsigned gm, unsigned ge,
                                             unsigned lm, unsigned le);

void prepare_replication_factor_update(
    const Key& key,
    map<Address, ReplicationFactorUpdate>& replication_factor_map,
    Address server_address, map<Key, KeyReplication>& key_replication_map);

void change_replication_factor(map<Key, KeyReplication>& requests,
                               map<TierId, GlobalHashRing>& global_hash_rings,
                               map<TierId, LocalHashRing>& local_hash_rings,
                               vector<Address>& routing_ips,
                               map<Key, KeyReplication>& key_replication_map,
                               SocketCache& pushers, MonitoringThread& mt,
                               zmq::socket_t& response_puller, logger log,
                               unsigned& rid);

void add_node(logger log, string tier, unsigned number, unsigned& adding,
              SocketCache& pushers, const Address& management_ip);

void remove_node(logger log, ServerThread& node, string tier,
                 bool& removing_flag, SocketCache& pushers,
                 map<Address, unsigned>& departing_node_map,
                 MonitoringThread& mt);

#endif  // KVS_INCLUDE_MONITOR_MONITORING_UTILS_HPP_
