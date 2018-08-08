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

#include <chrono>

#include "kvs/kvs_handlers.hpp"
#include "yaml-cpp/yaml.h"

// define server report threshold (in second)
const unsigned kServerReportThreshold = 15;

// define server's key monitoring threshold (in second)
const unsigned kKeyMonitoringThreshold = 60;

unsigned kThreadNum;

unsigned kSelfTierId;
std::vector<unsigned> kSelfTierIdVector;

unsigned kMemoryThreadCount;
unsigned kEbsThreadCount;

unsigned kDefaultGlobalMemoryReplication;
unsigned kDefaultGlobalEbsReplication;
unsigned kDefaultLocalReplication;

std::unordered_map<unsigned, TierData> kTierDataMap;

ZmqUtil zmq_util;
ZmqUtilInterface* kZmqUtil = &zmq_util;

HashRingUtil hash_ring_util;
HashRingUtilInterface* kHashRingUtil = &hash_ring_util;

void run(unsigned thread_id, Address public_ip, Address private_ip,
         Address seed_ip, std::vector<Address> routing_addresses,
         std::vector<Address> monitoring_addresses) {
  std::string log_file = "log_" + std::to_string(thread_id) + ".txt";
  std::string logger_name = "server_logger_" + std::to_string(thread_id);
  auto logger = spdlog::basic_logger_mt(logger_name, log_file, true);
  logger->flush_on(spdlog::level::info);

  // each thread has a handle to itself
  ServerThread wt = ServerThread(public_ip, private_ip, thread_id);

  unsigned seed = time(NULL);
  seed += thread_id;

  // prepare the zmq context
  zmq::context_t context(1);
  SocketCache pushers(&context, ZMQ_PUSH);

  // initialize hash ring maps
  std::unordered_map<unsigned, GlobalHashRing> global_hash_ring_map;
  std::unordered_map<unsigned, LocalHashRing> local_hash_ring_map;

  // for periodically redistributing data when node joins
  AddressKeysetMap join_addr_keyset_map;

  // keep track of which key should be removed when node joins
  std::unordered_set<Key> join_remove_set;

  // pending events for asynchrony
  PendingMap<PendingRequest> pending_request_map;
  PendingMap<PendingGossip> pending_gossip_map;

  std::unordered_map<Key, KeyInfo> placement;

  // request server addresses from the seed node
  zmq::socket_t addr_requester(context, ZMQ_REQ);
  addr_requester.connect(RoutingThread(seed_ip, 0).get_seed_connect_addr());
  kZmqUtil->send_string("join", &addr_requester);

  // receive and add all the addresses that seed node sent
  std::string serialized_addresses = kZmqUtil->recv_string(&addr_requester);
  TierMembership membership;
  membership.ParseFromString(serialized_addresses);

  // populate start time
  unsigned long long duration = membership.start_time();
  std::chrono::milliseconds dur(duration);
  std::chrono::system_clock::time_point start_time(dur);

  // populate addresses
  for (const auto& tier : membership.tiers()) {
    for (const auto server : tier.servers()) {
      global_hash_ring_map[tier.tier_id()].insert(server.public_ip(),
                                                  server.private_ip(), 0);
    }
  }

  // add itself to global hash ring
  global_hash_ring_map[kSelfTierId].insert(public_ip, private_ip, 0);

  // form local hash rings
  for (const auto& tier_pair : kTierDataMap) {
    for (unsigned tid = 0; tid < tier_pair.second.thread_number_; tid++) {
      local_hash_ring_map[tier_pair.first].insert(public_ip, private_ip, tid);
    }
  }

  // thread 0 notifies other servers that it has joined
  if (thread_id == 0) {
    for (const auto& global_pair : global_hash_ring_map) {
      unsigned tier_id = global_pair.first;
      GlobalHashRing hash_ring = global_pair.second;

      for (const ServerThread& st : hash_ring.get_unique_servers()) {
        if (st.get_private_ip().compare(private_ip) != 0) {
          kZmqUtil->send_string(
              std::to_string(kSelfTierId) + ":" + public_ip + ":" + private_ip,
              &pushers[st.get_node_join_connect_addr()]);
        }
      }
    }

    std::string msg = "join:" + std::to_string(kSelfTierId) + ":" + public_ip +
                      ":" + private_ip;

    // notify proxies that this node has joined
    for (const std::string& address : routing_addresses) {
      kZmqUtil->send_string(
          msg, &pushers[RoutingThread(address, 0).get_notify_connect_addr()]);
    }

    // notify monitoring nodes that this node has joined
    for (const std::string& address : monitoring_addresses) {
      kZmqUtil->send_string(
          msg, &pushers[MonitoringThread(address).get_notify_connect_addr()]);
    }
  }

  Serializer* serializer;

  if (kSelfTierId == 1) {
    MemoryKVS* kvs = new MemoryKVS();
    serializer = new MemorySerializer(kvs);
  } else if (kSelfTierId == 2) {
    serializer = new EBSSerializer(thread_id);
  } else {
    logger->info("Invalid node type");
    exit(1);
  }

  // the set of changes made on this thread since the last round of gossip
  std::unordered_set<Key> local_changeset;

  // keep track of the key stat
  std::unordered_map<Key, unsigned> key_size_map;
  // keep track of key access timestamp
  std::unordered_map<
      Key, std::multiset<std::chrono::time_point<std::chrono::system_clock>>>
      key_access_timestamp;
  // keep track of total access
  unsigned total_accesses;

  // listens for a new node joining
  zmq::socket_t join_puller(context, ZMQ_PULL);
  join_puller.bind(wt.get_node_join_bind_addr());

  // listens for a node departing
  zmq::socket_t depart_puller(context, ZMQ_PULL);
  depart_puller.bind(wt.get_node_depart_bind_addr());

  // responsible for listening for a command that this node should leave
  zmq::socket_t self_depart_puller(context, ZMQ_PULL);
  self_depart_puller.bind(wt.get_self_depart_bind_addr());

  // responsible for handling requests
  zmq::socket_t request_puller(context, ZMQ_PULL);
  request_puller.bind(wt.get_request_pulling_bind_addr());

  // responsible for processing gossip
  zmq::socket_t gossip_puller(context, ZMQ_PULL);
  gossip_puller.bind(wt.get_gossip_bind_addr());

  // responsible for listening for key replication factor response
  zmq::socket_t replication_factor_puller(context, ZMQ_PULL);
  replication_factor_puller.bind(wt.get_replication_factor_bind_addr());

  // responsible for listening for key replication factor change
  zmq::socket_t replication_factor_change_puller(context, ZMQ_PULL);
  replication_factor_change_puller.bind(
      wt.get_replication_factor_change_bind_addr());

  //  Initialize poll set
  std::vector<zmq::pollitem_t> pollitems = {
      {static_cast<void*>(join_puller), 0, ZMQ_POLLIN, 0},
      {static_cast<void*>(depart_puller), 0, ZMQ_POLLIN, 0},
      {static_cast<void*>(self_depart_puller), 0, ZMQ_POLLIN, 0},
      {static_cast<void*>(request_puller), 0, ZMQ_POLLIN, 0},
      {static_cast<void*>(gossip_puller), 0, ZMQ_POLLIN, 0},
      {static_cast<void*>(replication_factor_puller), 0, ZMQ_POLLIN, 0},
      {static_cast<void*>(replication_factor_change_puller), 0, ZMQ_POLLIN, 0}};

  auto gossip_start = std::chrono::system_clock::now();
  auto gossip_end = std::chrono::system_clock::now();
  auto report_start = std::chrono::system_clock::now();
  auto report_end = std::chrono::system_clock::now();

  unsigned long long working_time = 0;
  unsigned long long working_time_map[8] = {0, 0, 0, 0, 0, 0, 0, 0};
  unsigned epoch = 0;

  // enter event loop
  while (true) {
    kZmqUtil->poll(0, &pollitems);
    // receives a node join
    if (pollitems[0].revents & ZMQ_POLLIN) {
      auto work_start = std::chrono::system_clock::now();

      std::string serialized = kZmqUtil->recv_string(&join_puller);
      node_join_handler(thread_id, seed, public_ip, private_ip, logger,
                        serialized, global_hash_ring_map, local_hash_ring_map,
                        key_size_map, placement, join_remove_set, pushers, wt,
                        join_addr_keyset_map);

      auto time_elapsed = std::chrono::duration_cast<std::chrono::microseconds>(
                              std::chrono::system_clock::now() - work_start)
                              .count();
      working_time += time_elapsed;
      working_time_map[0] += time_elapsed;
    }

    if (pollitems[1].revents & ZMQ_POLLIN) {
      auto work_start = std::chrono::system_clock::now();

      std::string serialized = kZmqUtil->recv_string(&depart_puller);
      node_depart_handler(thread_id, public_ip, private_ip,
                          global_hash_ring_map, logger, serialized, pushers);

      auto time_elapsed = std::chrono::duration_cast<std::chrono::microseconds>(
                              std::chrono::system_clock::now() - work_start)
                              .count();
      working_time += time_elapsed;
      working_time_map[1] += time_elapsed;
    }

    if (pollitems[2].revents & ZMQ_POLLIN) {
      auto work_start = std::chrono::system_clock::now();

      std::string serialized = kZmqUtil->recv_string(&self_depart_puller);
      self_depart_handler(thread_id, seed, public_ip, private_ip, logger,
                          serialized, global_hash_ring_map, local_hash_ring_map,
                          key_size_map, placement, routing_addresses,
                          monitoring_addresses, wt, pushers, serializer);

      auto time_elapsed = std::chrono::duration_cast<std::chrono::microseconds>(
                              std::chrono::system_clock::now() - work_start)
                              .count();
      working_time += time_elapsed;
      working_time_map[2] += time_elapsed;
    }

    if (pollitems[3].revents & ZMQ_POLLIN) {
      auto work_start = std::chrono::system_clock::now();

      std::string serialized = kZmqUtil->recv_string(&request_puller);
      user_request_handler(total_accesses, seed, serialized, start_time, logger,
                           global_hash_ring_map, local_hash_ring_map,
                           key_size_map, pending_request_map,
                           key_access_timestamp, placement, local_changeset, wt,
                           serializer, pushers);

      auto time_elapsed = std::chrono::duration_cast<std::chrono::microseconds>(
                              std::chrono::system_clock::now() - work_start)
                              .count();
      working_time += time_elapsed;
      working_time_map[3] += time_elapsed;
    }

    // receive gossip
    if (pollitems[4].revents & ZMQ_POLLIN) {
      auto work_start = std::chrono::system_clock::now();

      std::string serialized = kZmqUtil->recv_string(&gossip_puller);
      gossip_handler(seed, serialized, global_hash_ring_map,
                     local_hash_ring_map, key_size_map, pending_gossip_map,
                     placement, wt, serializer, pushers);

      auto time_elapsed = std::chrono::duration_cast<std::chrono::microseconds>(
                              std::chrono::system_clock::now() - work_start)
                              .count();
      working_time += time_elapsed;
      working_time_map[4] += time_elapsed;
    }

    // receives replication factor response
    if (pollitems[5].revents & ZMQ_POLLIN) {
      auto work_start = std::chrono::system_clock::now();

      std::string serialized =
          kZmqUtil->recv_string(&replication_factor_puller);
      rep_factor_response_handler(
          seed, total_accesses, logger, serialized, start_time,
          global_hash_ring_map, local_hash_ring_map, pending_request_map,
          pending_gossip_map, key_access_timestamp, placement, key_size_map,
          local_changeset, wt, serializer, pushers);

      auto time_elapsed = std::chrono::duration_cast<std::chrono::microseconds>(
                              std::chrono::system_clock::now() - work_start)
                              .count();
      working_time += time_elapsed;
      working_time_map[5] += time_elapsed;
    }

    // receive replication factor change
    if (pollitems[6].revents & ZMQ_POLLIN) {
      auto work_start = std::chrono::system_clock::now();

      std::string serialized =
          kZmqUtil->recv_string(&replication_factor_change_puller);
      rep_factor_change_handler(public_ip, private_ip, thread_id, seed, logger,
                                serialized, global_hash_ring_map,
                                local_hash_ring_map, placement, key_size_map,
                                local_changeset, wt, serializer, pushers);

      auto time_elapsed = std::chrono::duration_cast<std::chrono::microseconds>(
                              std::chrono::system_clock::now() - work_start)
                              .count();
      working_time += time_elapsed;
      working_time_map[6] += time_elapsed;
    }

    // gossip updates to other threads
    gossip_end = std::chrono::system_clock::now();
    if (std::chrono::duration_cast<std::chrono::microseconds>(gossip_end -
                                                              gossip_start)
            .count() >= PERIOD) {
      auto work_start = std::chrono::system_clock::now();
      // only gossip if we have changes
      if (local_changeset.size() > 0) {
        AddressKeysetMap addr_keyset_map;

        bool succeed;
        for (const Key& key : local_changeset) {
          ServerThreadSet threads = kHashRingUtil->get_responsible_threads(
              wt.get_replication_factor_connect_addr(), key, is_metadata(key),
              global_hash_ring_map, local_hash_ring_map, placement, pushers,
              kAllTierIds, succeed, seed);

          if (succeed) {
            for (const ServerThread& thread : threads) {
              addr_keyset_map[thread.get_gossip_connect_addr()].insert(key);
            }
          } else {
            logger->error("Missing key replication factor in gossip routine.");
          }
        }

        send_gossip(addr_keyset_map, pushers, serializer);
        local_changeset.clear();
      }

      gossip_start = std::chrono::system_clock::now();
      auto time_elapsed = std::chrono::duration_cast<std::chrono::microseconds>(
                              std::chrono::system_clock::now() - work_start)
                              .count();

      working_time += time_elapsed;
      working_time_map[7] += time_elapsed;
    }

    // collect and store internal statistics
    report_end = std::chrono::system_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::seconds>(
                        report_end - report_start)
                        .count();

    if (duration >= kServerReportThreshold) {
      epoch += 1;

      Key key = get_metadata_key(wt, kSelfTierId, wt.get_tid(),
                                 MetadataType::server_stats);

      // compute total storage consumption
      unsigned long long consumption = 0;
      for (const auto& key_pair : key_size_map) {
        consumption += key_pair.second;
      }

      int index = 0;
      for (const unsigned long long& time : working_time_map) {
        // cast to microsecond
        double event_occupancy = (double)time / ((double)duration * 1000000);

        if (event_occupancy > 0.02) {
          logger->info("Event {} occupancy is {}.", std::to_string(index++),
                       std::to_string(event_occupancy));
        }
      }

      double occupancy = (double)working_time / ((double)duration * 1000000);
      if (occupancy > 0.02) {
        logger->info("Occupancy is {}.", std::to_string(occupancy));
      }

      ServerThreadStatistics stat;
      stat.set_storage_consumption(consumption / 1000);  // cast to KB
      stat.set_occupancy(occupancy);
      stat.set_epoch(epoch);
      stat.set_total_accesses(total_accesses);

      std::string serialized_stat;
      stat.SerializeToString(&serialized_stat);

      KeyRequest req;
      req.set_type(get_request_type("PUT"));
      prepare_put_tuple(req, key, serialized_stat, 0);

      auto threads = kHashRingUtil->get_responsible_threads_metadata(
          key, global_hash_ring_map[1], local_hash_ring_map[1]);
      if (threads.size() != 0) {
        Address target_address =
            next(begin(threads), rand_r(&seed) % threads.size())
                ->get_request_pulling_connect_addr();
        std::string serialized;
        req.SerializeToString(&serialized);
        kZmqUtil->send_string(serialized, &pushers[target_address]);
      }

      // compute key access stats
      KeyAccessData access;
      auto current_time = std::chrono::system_clock::now();

      for (const auto& key_access_pair : key_access_timestamp) {
        Key key = key_access_pair.first;
        auto access_times = key_access_pair.second;

        // garbage collect
        for (const auto& time : access_times) {
          if (std::chrono::duration_cast<std::chrono::seconds>(current_time -
                                                               time)
                  .count() >= kKeyMonitoringThreshold) {
            access_times.erase(time);
            break;
          }
        }

        // update key_access_frequency
        KeyAccessData_KeyCount* tp = access.add_keys();
        tp->set_key(key);
        tp->set_access_count(access_times.size());
      }

      // report key access stats
      key = get_metadata_key(wt, kSelfTierId, wt.get_tid(),
                             MetadataType::key_access);
      std::string serialized_access;
      access.SerializeToString(&serialized_access);

      req.Clear();
      req.set_type(get_request_type("PUT"));
      prepare_put_tuple(req, key, serialized_access, 0);

      threads = kHashRingUtil->get_responsible_threads_metadata(
          key, global_hash_ring_map[1], local_hash_ring_map[1]);

      if (threads.size() != 0) {
        Address target_address =
            next(begin(threads), rand_r(&seed) % threads.size())
                ->get_request_pulling_connect_addr();
        std::string serialized;
        req.SerializeToString(&serialized);
        kZmqUtil->send_string(serialized, &pushers[target_address]);
      }

      // report key size stats
      KeySizeData primary_key_size;
      for (const auto& key_size_pair : key_size_map) {
        if (is_primary_replica(key_size_pair.first, placement,
                               global_hash_ring_map, local_hash_ring_map, wt)) {
          KeySizeData_KeySize* ks = primary_key_size.add_key_sizes();
          ks->set_key(key_size_pair.first);
          ks->set_size(key_size_pair.second);
        }
      }

      key = get_metadata_key(wt, kSelfTierId, wt.get_tid(),
                             MetadataType::key_size);

      std::string serialized_size;
      primary_key_size.SerializeToString(&serialized_size);

      req.Clear();
      req.set_type(get_request_type("PUT"));
      prepare_put_tuple(req, key, serialized_size, 0);

      threads = kHashRingUtil->get_responsible_threads_metadata(
          key, global_hash_ring_map[1], local_hash_ring_map[1]);

      if (threads.size() != 0) {
        Address target_address =
            next(begin(threads), rand_r(&seed) % threads.size())
                ->get_request_pulling_connect_addr();
        std::string serialized;
        req.SerializeToString(&serialized);
        kZmqUtil->send_string(serialized, &pushers[target_address]);
      }

      report_start = std::chrono::system_clock::now();

      // reset stats tracked in memory
      working_time = 0;
      total_accesses = 0;
      memset(working_time_map, 0, sizeof(working_time_map));
    }

    // redistribute data after node joins
    if (join_addr_keyset_map.size() != 0) {
      std::unordered_set<Address> remove_address_set;

      // assemble gossip
      AddressKeysetMap addr_keyset_map;
      for (const auto& join_pair : join_addr_keyset_map) {
        Address address = join_pair.first;
        std::unordered_set<Key> key_set = join_pair.second;
        unsigned count = 0;

        // track all sent keys because we cannot modify the key_set while
        // iterating over it
        std::unordered_set<Key> sent_keys;

        for (const Key& key : key_set) {
          addr_keyset_map[address].insert(key);
          sent_keys.insert(key);
          count++;
        }

        // remove the keys we just dealt with
        for (const Key& key : sent_keys) {
          key_set.erase(key);
        }

        if (key_set.size() == 0) {
          remove_address_set.insert(address);
        }
      }

      for (const Address& remove_address : remove_address_set) {
        join_addr_keyset_map.erase(remove_address);
      }

      send_gossip(addr_keyset_map, pushers, serializer);

      // remove keys
      if (join_addr_keyset_map.size() == 0) {
        for (const std::string& key : join_remove_set) {
          key_size_map.erase(key);
          serializer->remove(key);
        }
      }
    }
  }
}

int main(int argc, char* argv[]) {
  if (argc != 1) {
    std::cerr << "Usage: " << argv[0] << std::endl;
    return 1;
  }

  // populate metadata
  char* stype = getenv("SERVER_TYPE");
  if (stype != NULL) {
    kSelfTierId = atoi(stype);
  } else {
    std::cout
        << "No server type specified. The default behavior is to start the "
           "server in memory mode."
        << std::endl;
    kSelfTierId = 1;
  }

  kSelfTierIdVector = {kSelfTierId};

  // read the YAML conf
  YAML::Node conf = YAML::LoadFile("conf/kvs-config.yml");
  YAML::Node threads = conf["threads"];
  kMemoryThreadCount = threads["memory"].as<unsigned>();
  kEbsThreadCount = threads["ebs"].as<unsigned>();

  YAML::Node replication = conf["replication"];
  kDefaultGlobalMemoryReplication = replication["memory"].as<unsigned>();
  kDefaultGlobalEbsReplication = replication["ebs"].as<unsigned>();
  kDefaultLocalReplication = replication["local"].as<unsigned>();

  YAML::Node server = conf["server"];
  Address public_ip = server["public_ip"].as<std::string>();
  Address private_ip = server["private_ip"].as<std::string>();

  std::vector<Address> routing_addresses;
  std::vector<Address> monitoring_addresses;

  Address seed_ip = server["seed_ip"].as<std::string>();
  YAML::Node monitoring = server["monitoring"];
  YAML::Node routing = server["routing"];

  for (const YAML::Node& address : routing) {
    routing_addresses.push_back(address.as<Address>());
  }

  for (const YAML::Node& address : monitoring) {
    monitoring_addresses.push_back(address.as<Address>());
  }

  kTierDataMap[1] = TierData(
      kMemoryThreadCount, kDefaultGlobalMemoryReplication, kMemoryNodeCapacity);
  kTierDataMap[2] =
      TierData(kEbsThreadCount, kDefaultGlobalEbsReplication, kEbsNodeCapacity);

  kThreadNum = kTierDataMap[kSelfTierId].thread_number_;

  // start the initial threads based on kThreadNum
  std::vector<std::thread> worker_threads;
  for (unsigned thread_id = 1; thread_id < kThreadNum; thread_id++) {
    worker_threads.push_back(std::thread(run, thread_id, public_ip, private_ip,
                                         seed_ip, routing_addresses,
                                         monitoring_addresses));
  }

  run(0, public_ip, private_ip, seed_ip, routing_addresses,
      monitoring_addresses);
}
