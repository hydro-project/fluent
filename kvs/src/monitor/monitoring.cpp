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
unsigned kSharedMemoryThreadCount;

unsigned kDefaultGlobalMemoryReplication;
unsigned kDefaultGlobalEbsReplication;
unsigned kDefaultSharedMemoryReplication;
unsigned kDefaultLocalReplication;
unsigned kMinimumReplicaNumber;

// read-only per-tier metadata
std::unordered_map<unsigned, TierData> kTierDataMap;

ZmqUtil zmq_util;
ZmqUtilInterface *kZmqUtil = &zmq_util;

HashRingUtil hash_ring_util;
HashRingUtilInterface *kHashRingUtil = &hash_ring_util;

int main(int argc, char *argv[]) {
  auto logger = spdlog::basic_logger_mt("monitoring_logger", "log.txt", true);
  logger->flush_on(spdlog::level::info);

  if (argc != 1) {
    std::cerr << "Usage: " << argv[0] << std::endl;
    return 1;
  }

  // read the YAML conf
  YAML::Node conf = YAML::LoadFile("conf/kvs-config.yml");
  YAML::Node monitoring = conf["monitoring"];
  Address ip = monitoring["ip"].as<Address>();
  Address management_address = monitoring["mgmt_ip"].as<Address>();

  YAML::Node threads = conf["threads"];
  kMemoryThreadCount = threads["memory"].as<unsigned>();
  kEbsThreadCount = threads["ebs"].as<unsigned>();
  kSharedMemoryThreadCount = threads["sharedmemory"].as<unsigned>();

  YAML::Node replication = conf["replication"];
  kDefaultGlobalMemoryReplication = replication["memory"].as<unsigned>();
  kDefaultGlobalEbsReplication = replication["ebs"].as<unsigned>();
  kDefaultSharedMemoryReplication = replication["sharedmemory"].as<unsigned>();
  kDefaultLocalReplication = replication["local"].as<unsigned>();
  kMinimumReplicaNumber = replication["minimum"].as<unsigned>();

  kTierDataMap[1] = TierData(
      kMemoryThreadCount, kDefaultGlobalMemoryReplication, kMemoryNodeCapacity);
  kTierDataMap[2] =
      TierData(kEbsThreadCount, kDefaultGlobalEbsReplication, kEbsNodeCapacity);

  // initialize hash ring maps
  std::unordered_map<unsigned, GlobalHashRing> global_hash_ring_map;
  std::unordered_map<unsigned, LocalHashRing> local_hash_ring_map;

  // form local hash rings
  for (const auto &tier_pair : kTierDataMap) {
    for (unsigned tid = 0; tid < tier_pair.second.thread_number_; tid++) {
      local_hash_ring_map[tier_pair.first].insert(ip, ip, 0, tid,
                                                  tier_pair.first);
    }
  }

  // keep track of the keys' replication info
  std::unordered_map<Key, KeyInfo> placement;
  // warm up for benchmark
  // warmup_placement_to_defaults(placement);

  unsigned memory_node_number;
  unsigned ebs_node_number;
  // keep track of the keys' access by worker address
  std::unordered_map<Key, std::unordered_map<Address, unsigned>>
      key_access_frequency;
  // keep track of the keys' access summary
  std::unordered_map<Key, unsigned> key_access_summary;
  // keep track of the size of each key-value pair
  std::unordered_map<Key, unsigned> key_size;
  // keep track of memory tier storage consumption
  StorageStat memory_tier_storage;
  // keep track of ebs tier storage consumption
  StorageStat ebs_tier_storage;
  // keep track of memory tier thread occupancy
  OccupancyStats memory_tier_occupancy;
  // keep track of ebs tier thread occupancy
  OccupancyStats ebs_tier_occupancy;
  // keep track of memory tier hit
  AccessStat memory_tier_access;
  // keep track of ebs tier hit
  AccessStat ebs_tier_access;
  // keep track of some summary statistics
  SummaryStats ss;
  // keep track of user latency info
  std::unordered_map<std::string, double> user_latency;
  // keep track of user throughput info
  std::unordered_map<std::string, double> user_throughput;
  // used for adjusting the replication factors based on feedback from the user
  std::unordered_map<Key, std::pair<double, unsigned>> latency_miss_ratio_map;

  std::vector<Address> routing_address;

  MonitoringThread mt = MonitoringThread(ip);

  zmq::context_t context(1);
  SocketCache pushers(&context, ZMQ_PUSH);

  // responsible for listening to the response of the replication factor change
  // request
  zmq::socket_t response_puller(context, ZMQ_PULL);
  int timeout = 10000;

  response_puller.setsockopt(ZMQ_RCVTIMEO, &timeout, sizeof(timeout));
  response_puller.bind(mt.get_request_pulling_bind_addr());

  // keep track of departing node status
  std::unordered_map<Address, unsigned> departing_node_map;

  // responsible for both node join and departure
  zmq::socket_t notify_puller(context, ZMQ_PULL);
  notify_puller.bind(mt.get_notify_bind_addr());

  // responsible for receiving depart done notice
  zmq::socket_t depart_done_puller(context, ZMQ_PULL);
  depart_done_puller.bind(mt.get_depart_done_bind_addr());

  // responsible for receiving feedback from users
  zmq::socket_t feedback_puller(context, ZMQ_PULL);
  feedback_puller.bind(mt.get_latency_report_bind_addr());

  std::vector<zmq::pollitem_t> pollitems = {
      {static_cast<void *>(notify_puller), 0, ZMQ_POLLIN, 0},
      {static_cast<void *>(depart_done_puller), 0, ZMQ_POLLIN, 0},
      {static_cast<void *>(feedback_puller), 0, ZMQ_POLLIN, 0}};

  auto report_start = std::chrono::system_clock::now();
  auto report_end = std::chrono::system_clock::now();

  auto grace_start = std::chrono::system_clock::now();

  unsigned adding_memory_node = 0;
  unsigned adding_ebs_node = 0;
  bool removing_memory_node = false;
  bool removing_ebs_node = false;

  unsigned server_monitoring_epoch = 0;

  unsigned rid = 0;

  while (true) {
    // listen for ZMQ events
    kZmqUtil->poll(0, &pollitems);

    // handle a join or depart event
    if (pollitems[0].revents & ZMQ_POLLIN) {
      std::string serialized = kZmqUtil->recv_string(&notify_puller);
      membership_handler(logger, serialized, global_hash_ring_map,
                         adding_memory_node, adding_ebs_node, grace_start,
                         routing_address, memory_tier_storage, ebs_tier_storage,
                         memory_tier_occupancy, ebs_tier_occupancy,
                         key_access_frequency);
    }

    // handle a depart done notification
    if (pollitems[1].revents & ZMQ_POLLIN) {
      std::string serialized = kZmqUtil->recv_string(&depart_done_puller);
      depart_done_handler(logger, serialized, departing_node_map,
                          management_address, removing_memory_node,
                          removing_ebs_node, pushers, grace_start);
    }

    if (pollitems[2].revents & ZMQ_POLLIN) {
      std::string serialized = kZmqUtil->recv_string(&feedback_puller);
      feedback_handler(serialized, user_latency, user_throughput,
                       latency_miss_ratio_map);
    }

    report_end = std::chrono::system_clock::now();

    if (std::chrono::duration_cast<std::chrono::seconds>(report_end -
                                                         report_start)
            .count() >= kMonitoringThreshold) {
      server_monitoring_epoch += 1;

      memory_node_number = global_hash_ring_map[1].size() / kVirtualThreadNum;
      ebs_node_number = global_hash_ring_map[2].size() / kVirtualThreadNum;
      // clear stats
      key_access_frequency.clear();
      key_access_summary.clear();

      memory_tier_storage.clear();
      ebs_tier_storage.clear();

      memory_tier_occupancy.clear();
      ebs_tier_occupancy.clear();

      ss.clear();

      user_latency.clear();
      user_throughput.clear();
      latency_miss_ratio_map.clear();

      // collect internal statistics
      collect_internal_stats(
          global_hash_ring_map, local_hash_ring_map, pushers, mt,
          response_puller, logger, rid, key_access_frequency, key_size,
          memory_tier_storage, ebs_tier_storage, memory_tier_occupancy,
          ebs_tier_occupancy, memory_tier_access, ebs_tier_access);

      // compute summary statistics
      compute_summary_stats(key_access_frequency, memory_tier_storage,
                            ebs_tier_storage, memory_tier_occupancy,
                            ebs_tier_occupancy, memory_tier_access,
                            ebs_tier_access, key_access_summary, ss, logger,
                            server_monitoring_epoch);

      // collect external statistics
      collect_external_stats(user_latency, user_throughput, ss, logger);

      // initialize replication factor for new keys
      for (const auto &key_access_pair : key_access_summary) {
        Key key = key_access_pair.first;
        if (!is_metadata(key) && placement.find(key) == placement.end()) {
          init_replication(placement, key);
        }
      }

      // execute policies
      storage_policy(logger, global_hash_ring_map, grace_start, ss,
                     memory_node_number, ebs_node_number, adding_memory_node,
                     adding_ebs_node, removing_ebs_node, management_address, mt,
                     departing_node_map, pushers);

      movement_policy(logger, global_hash_ring_map, local_hash_ring_map,
                      grace_start, ss, memory_node_number, ebs_node_number,
                      adding_memory_node, adding_ebs_node, management_address,
                      placement, key_access_summary, key_size, mt, pushers,
                      response_puller, routing_address, rid);

      slo_policy(logger, global_hash_ring_map, local_hash_ring_map, grace_start,
                 ss, memory_node_number, adding_memory_node,
                 removing_memory_node, management_address, placement,
                 key_access_summary, mt, departing_node_map, pushers,
                 response_puller, routing_address, rid, latency_miss_ratio_map);

      report_start = std::chrono::system_clock::now();
    }
  }
}
