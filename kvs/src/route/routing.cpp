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

#include "route/routing_handlers.hpp"
#include "yaml-cpp/yaml.h"

std::unordered_map<unsigned, TierData> kTierDataMap;
unsigned kDefaultLocalReplication;
unsigned kRoutingThreadCount;

ZmqUtil zmq_util;
ZmqUtilInterface *kZmqUtil = &zmq_util;

HashRingUtil hash_ring_util;
HashRingUtilInterface *kHashRingUtil = &hash_ring_util;

void run(unsigned thread_id, Address ip,
         std::vector<Address> monitoring_addresses) {
  std::string log_file = "log_" + std::to_string(thread_id) + ".txt";
  std::string logger_name = "routing_logger_" + std::to_string(thread_id);
  auto logger = spdlog::basic_logger_mt(logger_name, log_file, true);
  logger->flush_on(spdlog::level::info);

  RoutingThread rt = RoutingThread(ip, thread_id);

  unsigned seed = time(NULL);
  seed += thread_id;

  // prepare the zmq context
  zmq::context_t context(1);
  SocketCache pushers(&context, ZMQ_PUSH);
  std::unordered_map<Key, KeyInfo> placement;

  // warm up for benchmark
  // warmup_placement_to_defaults(placement);

  if (thread_id == 0) {
    // notify monitoring nodes
    for (const std::string &address : monitoring_addresses) {
      kZmqUtil->send_string(
          // add null because it expects two IPs from server nodes...
          "join:0:" + ip + ":NULL",
          &pushers[MonitoringThread(address).get_notify_connect_addr()]);
    }
  }

  // initialize hash ring maps
  std::unordered_map<unsigned, GlobalHashRing> global_hash_ring_map;
  std::unordered_map<unsigned, LocalHashRing> local_hash_ring_map;

  // pending events for asynchrony
  PendingMap<std::pair<Address, std::string>> pending_key_request_map;

  // form local hash rings
  for (const auto &tier_pair : kTierDataMap) {
    if (tier_pair.first != 3) {
      for (unsigned tid = 0; tid < tier_pair.second.thread_number_; tid++) {
        local_hash_ring_map[tier_pair.first].insert(ip, ip, tid);
      }
    }
  }

  // responsible for sending existing server addresses to a new node (relevant
  // to seed node)
  zmq::socket_t addr_responder(context, ZMQ_REP);
  addr_responder.bind(rt.get_seed_bind_addr());

  // responsible for both node join and departure
  zmq::socket_t notify_puller(context, ZMQ_PULL);
  notify_puller.bind(rt.get_notify_bind_addr());

  // responsible for listening for key replication factor response
  zmq::socket_t replication_factor_puller(context, ZMQ_PULL);
  replication_factor_puller.bind(rt.get_replication_factor_bind_addr());

  // responsible for handling key replication factor change requests from server
  // nodes
  zmq::socket_t replication_factor_change_puller(context, ZMQ_PULL);
  replication_factor_change_puller.bind(
      rt.get_replication_factor_change_bind_addr());

  // responsible for handling key address request from users
  zmq::socket_t key_address_puller(context, ZMQ_PULL);
  key_address_puller.bind(rt.get_key_address_bind_addr());

  std::vector<zmq::pollitem_t> pollitems = {
      {static_cast<void *>(addr_responder), 0, ZMQ_POLLIN, 0},
      {static_cast<void *>(notify_puller), 0, ZMQ_POLLIN, 0},
      {static_cast<void *>(replication_factor_puller), 0, ZMQ_POLLIN, 0},
      {static_cast<void *>(replication_factor_change_puller), 0, ZMQ_POLLIN, 0},
      {static_cast<void *>(key_address_puller), 0, ZMQ_POLLIN, 0}};

  auto start_time = std::chrono::system_clock::now();
  auto start_time_ms =
      std::chrono::time_point_cast<std::chrono::milliseconds>(start_time);

  auto value = start_time_ms.time_since_epoch();
  unsigned long long duration = value.count();

  while (true) {
    kZmqUtil->poll(-1, &pollitems);

    // only relavant for the seed node
    if (pollitems[0].revents & ZMQ_POLLIN) {
      kZmqUtil->recv_string(&addr_responder);
      auto serialized = seed_handler(logger, global_hash_ring_map, duration);
      kZmqUtil->send_string(serialized, &addr_responder);
    }

    // handle a join or depart event coming from the server side
    if (pollitems[1].revents & ZMQ_POLLIN) {
      std::string serialized = kZmqUtil->recv_string(&notify_puller);
      membership_handler(logger, serialized, pushers, global_hash_ring_map,
                         thread_id, ip);
    }

    // received replication factor response
    if (pollitems[2].revents & ZMQ_POLLIN) {
      std::string serialized =
          kZmqUtil->recv_string(&replication_factor_puller);
      replication_response_handler(logger, serialized, pushers, rt,
                                   global_hash_ring_map, local_hash_ring_map,
                                   placement, pending_key_request_map, seed);
    }

    if (pollitems[3].revents & ZMQ_POLLIN) {
      std::string serialized =
          kZmqUtil->recv_string(&replication_factor_change_puller);
      replication_change_handler(logger, serialized, pushers, placement,
                                 thread_id, ip);
    }

    if (pollitems[4].revents & ZMQ_POLLIN) {
      std::string serialized = kZmqUtil->recv_string(&key_address_puller);
      address_handler(logger, serialized, pushers, rt, global_hash_ring_map,
                      local_hash_ring_map, placement, pending_key_request_map,
                      seed);
    }
  }
}

int main(int argc, char *argv[]) {
  if (argc != 1) {
    std::cerr << "Usage: " << argv[0] << std::endl;
    return 1;
  }

  YAML::Node conf = YAML::LoadFile("conf/kvs-config.yml");
  YAML::Node threads = conf["threads"];
  unsigned kMemoryThreadCount = threads["memory"].as<unsigned>();
  unsigned kEbsThreadCount = threads["ebs"].as<unsigned>();
  unsigned kSharedMemoryThreadCount = threads["shared"].as<unsigned>();
  kRoutingThreadCount = threads["routing"].as<unsigned>();

  YAML::Node replication = conf["replication"];
  unsigned kDefaultGlobalMemoryReplication =
      replication["memory"].as<unsigned>();
  unsigned kDefaultGlobalEbsReplication = replication["ebs"].as<unsigned>();
  unsigned kDefaultSharedMemoryReplication = replication["shared"].as<unsigned>();
  kDefaultLocalReplication = replication["local"].as<unsigned>();

  YAML::Node routing = conf["routing"];
  Address ip = routing["ip"].as<std::string>();
  std::vector<Address> monitoring_addresses;

  for (const YAML::Node &node : routing["monitoring"]) {
    std::string address = node.as<Address>();
    monitoring_addresses.push_back(address);
  }

  kTierDataMap[1] = TierData(
      kMemoryThreadCount, kDefaultGlobalMemoryReplication, kMemoryNodeCapacity);
  kTierDataMap[2] =
      TierData(kEbsThreadCount, kDefaultGlobalEbsReplication, kEbsNodeCapacity);
  kTierDataMap[3] =
      TierData(kSharedMemoryThreadCount, kDefaultSharedMemoryReplication, kSharedMemoryNodeCapacity);

  std::vector<std::thread> routing_worker_threads;

  for (unsigned thread_id = 1; thread_id < kRoutingThreadCount; thread_id++) {
    routing_worker_threads.push_back(
        std::thread(run, thread_id, ip, monitoring_addresses));
  }

  run(0, ip, monitoring_addresses);
}
