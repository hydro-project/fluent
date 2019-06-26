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
#include "monitor/monitoring_utils.hpp"
#include "monitor/policies.hpp"
#include "yaml-cpp/yaml.h"

unsigned kMemoryThreadCount;
unsigned kEbsThreadCount;

unsigned kMemoryNodeCapacity;
unsigned kEbsNodeCapacity;

unsigned kDefaultGlobalMemoryReplication;
unsigned kDefaultGlobalEbsReplication;
unsigned kDefaultLocalReplication;
unsigned kMinimumReplicaNumber;

bool kEnableElasticity;
bool kEnableTiering;
bool kEnableSelectiveRep;

// read-only per-tier metadata
map<TierId, TierMetadata> kTierMetadata;

ZmqUtil zmq_util;
ZmqUtilInterface *kZmqUtil = &zmq_util;

HashRingUtil hash_ring_util;
HashRingUtilInterface *kHashRingUtil = &hash_ring_util;

int main(int argc, char *argv[]) {
  auto log = spdlog::basic_logger_mt("monitoring_log", "log.txt", true);
  log->flush_on(spdlog::level::info);

  if (argc != 1) {
    std::cerr << "Usage: " << argv[0] << std::endl;
    return 1;
  }

  // read the YAML conf
  YAML::Node conf = YAML::LoadFile("conf/kvs-config.yml");
  YAML::Node monitoring = conf["monitoring"];
  Address ip = monitoring["ip"].as<Address>();
  Address management_ip = monitoring["mgmt_ip"].as<Address>();

  YAML::Node policy = conf["policy"];
  kEnableElasticity = policy["elasticity"].as<bool>();
  kEnableSelectiveRep = policy["selective-rep"].as<bool>();
  kEnableTiering = policy["tiering"].as<bool>();

  log->info("Elasticity policy enabled: {}", kEnableElasticity);
  log->info("Tiering policy enabled: {}", kEnableTiering);
  log->info("Selective replication policy enabled: {}", kEnableSelectiveRep);

  YAML::Node threads = conf["threads"];
  kMemoryThreadCount = threads["memory"].as<unsigned>();
  kEbsThreadCount = threads["ebs"].as<unsigned>();

  YAML::Node capacities = conf["capacities"];
  kMemoryNodeCapacity = capacities["memory-cap"].as<unsigned>() * 1000000;
  kEbsNodeCapacity = capacities["ebs-cap"].as<unsigned>() * 1000000;

  YAML::Node replication = conf["replication"];
  kDefaultGlobalMemoryReplication = replication["memory"].as<unsigned>();
  kDefaultGlobalEbsReplication = replication["ebs"].as<unsigned>();
  kDefaultLocalReplication = replication["local"].as<unsigned>();
  kMinimumReplicaNumber = replication["minimum"].as<unsigned>();

  kTierMetadata[kMemoryTierId] =
      TierMetadata(kMemoryTierId, kMemoryThreadCount,
                   kDefaultGlobalMemoryReplication, kMemoryNodeCapacity);
  kTierMetadata[kEbsTierId] =
      TierMetadata(kEbsTierId, kEbsThreadCount, kDefaultGlobalEbsReplication,
                   kEbsNodeCapacity);

  // initialize hash ring maps
  map<TierId, GlobalHashRing> global_hash_rings;
  map<TierId, LocalHashRing> local_hash_rings;

  // form local hash rings
  for (const auto &pair : kTierMetadata) {
    TierMetadata tier = pair.second;
    for (unsigned tid = 0; tid < tier.thread_number_; tid++) {
      local_hash_rings[tier.id_].insert(ip, ip, 0, tid);
    }
  }

  // keep track of the keys' replication info
  map<Key, KeyReplication> key_replication_map;

  unsigned memory_node_count;
  unsigned ebs_node_count;

  map<Key, map<Address, unsigned>> key_access_frequency;

  map<Key, unsigned> key_access_summary;

  map<Key, unsigned> key_size;

  StorageStats memory_storage;

  StorageStats ebs_storage;

  OccupancyStats memory_occupancy;

  OccupancyStats ebs_occupancy;

  AccessStats memory_accesses;

  AccessStats ebs_accesses;

  SummaryStats ss;

  map<string, double> user_latency;

  map<string, double> user_throughput;

  map<Key, std::pair<double, unsigned>> latency_miss_ratio_map;

  vector<Address> routing_ips;

  MonitoringThread mt = MonitoringThread(ip);

  zmq::context_t context(1);
  SocketCache pushers(&context, ZMQ_PUSH);

  // responsible for listening to the response of the replication factor change
  // request
  zmq::socket_t response_puller(context, ZMQ_PULL);
  int timeout = 10000;

  response_puller.setsockopt(ZMQ_RCVTIMEO, &timeout, sizeof(timeout));
  response_puller.bind(mt.response_bind_address());

  // keep track of departing node status
  map<Address, unsigned> departing_node_map;

  // responsible for both node join and departure
  zmq::socket_t notify_puller(context, ZMQ_PULL);
  notify_puller.bind(mt.notify_bind_address());

  // responsible for receiving depart done notice
  zmq::socket_t depart_done_puller(context, ZMQ_PULL);
  depart_done_puller.bind(mt.depart_done_bind_address());

  // responsible for receiving feedback from users
  zmq::socket_t feedback_puller(context, ZMQ_PULL);
  feedback_puller.bind(mt.latency_report_bind_address());

  vector<zmq::pollitem_t> pollitems = {
      {static_cast<void *>(notify_puller), 0, ZMQ_POLLIN, 0},
      {static_cast<void *>(depart_done_puller), 0, ZMQ_POLLIN, 0},
      {static_cast<void *>(feedback_puller), 0, ZMQ_POLLIN, 0}};

  auto report_start = std::chrono::system_clock::now();
  auto report_end = std::chrono::system_clock::now();

  auto grace_start = std::chrono::system_clock::now();

  unsigned new_memory_count = 0;
  unsigned new_ebs_count = 0;
  bool removing_memory_node = false;
  bool removing_ebs_node = false;

  unsigned server_monitoring_epoch = 0;

  unsigned rid = 0;

  while (true) {
    kZmqUtil->poll(0, &pollitems);

    if (pollitems[0].revents & ZMQ_POLLIN) {
      string serialized = kZmqUtil->recv_string(&notify_puller);
      membership_handler(log, serialized, global_hash_rings, new_memory_count,
                         new_ebs_count, grace_start, routing_ips,
                         memory_storage, ebs_storage, memory_occupancy,
                         ebs_occupancy, key_access_frequency);
    }

    if (pollitems[1].revents & ZMQ_POLLIN) {
      string serialized = kZmqUtil->recv_string(&depart_done_puller);
      depart_done_handler(log, serialized, departing_node_map, management_ip,
                          removing_memory_node, removing_ebs_node, pushers,
                          grace_start);
    }

    if (pollitems[2].revents & ZMQ_POLLIN) {
      string serialized = kZmqUtil->recv_string(&feedback_puller);
      feedback_handler(serialized, user_latency, user_throughput,
                       latency_miss_ratio_map);
    }

    report_end = std::chrono::system_clock::now();

    if (std::chrono::duration_cast<std::chrono::seconds>(report_end -
                                                         report_start)
            .count() >= kMonitoringThreshold) {
      server_monitoring_epoch += 1;

      memory_node_count =
          global_hash_rings[kMemoryTierId].size() / kVirtualThreadNum;
      ebs_node_count = global_hash_rings[kEbsTierId].size() / kVirtualThreadNum;

      key_access_frequency.clear();
      key_access_summary.clear();

      memory_storage.clear();
      ebs_storage.clear();

      memory_occupancy.clear();
      ebs_occupancy.clear();

      ss.clear();

      user_latency.clear();
      user_throughput.clear();
      latency_miss_ratio_map.clear();

      collect_internal_stats(
          global_hash_rings, local_hash_rings, pushers, mt, response_puller,
          log, rid, key_access_frequency, key_size, memory_storage, ebs_storage,
          memory_occupancy, ebs_occupancy, memory_accesses, ebs_accesses);

      compute_summary_stats(key_access_frequency, memory_storage, ebs_storage,
                            memory_occupancy, ebs_occupancy, memory_accesses,
                            ebs_accesses, key_access_summary, ss, log,
                            server_monitoring_epoch);

      collect_external_stats(user_latency, user_throughput, ss, log);

      // initialize replication factor for new keys
      for (const auto &key_access_pair : key_access_summary) {
        Key key = key_access_pair.first;
        if (!is_metadata(key) &&
            key_replication_map.find(key) == key_replication_map.end()) {
          init_replication(key_replication_map, key);
        }
      }

      storage_policy(log, global_hash_rings, grace_start, ss, memory_node_count,
                     ebs_node_count, new_memory_count, new_ebs_count,
                     removing_ebs_node, management_ip, mt, departing_node_map,
                     pushers);

      movement_policy(log, global_hash_rings, local_hash_rings, grace_start, ss,
                      memory_node_count, ebs_node_count, new_memory_count,
                      new_ebs_count, management_ip, key_replication_map,
                      key_access_summary, key_size, mt, pushers,
                      response_puller, routing_ips, rid);

      slo_policy(log, global_hash_rings, local_hash_rings, grace_start, ss,
                 memory_node_count, new_memory_count, removing_memory_node,
                 management_ip, key_replication_map, key_access_summary, mt,
                 departing_node_map, pushers, response_puller, routing_ips, rid,
                 latency_miss_ratio_map);

      report_start = std::chrono::system_clock::now();
    }
  }
}
