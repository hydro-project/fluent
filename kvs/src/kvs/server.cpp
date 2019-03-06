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

#include "kvs/kvs_handlers.hpp"
#include "yaml-cpp/yaml.h"

// define server report threshold (in second)
const unsigned kServerReportThreshold = 15;

// define server's key monitoring threshold (in second)
const unsigned kKeyMonitoringThreshold = 60;

unsigned kThreadNum;

unsigned kSelfTierId;
vector<unsigned> kSelfTierIdVector;

unsigned kMemoryThreadCount;
unsigned kEbsThreadCount;

unsigned kMemoryNodeCapacity;
unsigned kEbsNodeCapacity;

unsigned kDefaultGlobalMemoryReplication;
unsigned kDefaultGlobalEbsReplication;
unsigned kDefaultLocalReplication;

map<TierId, TierMetadata> kTierMetadata;

ZmqUtil zmq_util;
ZmqUtilInterface* kZmqUtil = &zmq_util;

HashRingUtil hash_ring_util;
HashRingUtilInterface* kHashRingUtil = &hash_ring_util;

void run(unsigned thread_id, Address public_ip, Address private_ip,
         Address seed_ip, vector<Address> routing_ips,
         vector<Address> monitoring_ips, Address management_ip) {
  string log_file = "log_" + std::to_string(thread_id) + ".txt";
  string log_name = "server_log_" + std::to_string(thread_id);
  auto log = spdlog::basic_logger_mt(log_name, log_file, true);
  log->flush_on(spdlog::level::info);

  // each thread has a handle to itself
  ServerThread wt = ServerThread(public_ip, private_ip, thread_id);

  unsigned seed = time(NULL);
  seed += thread_id;

  // A monotonically increasing integer.
  unsigned rid = 0;

  // prepare the zmq context
  zmq::context_t context(1);
  SocketCache pushers(&context, ZMQ_PUSH);

  // initialize hash ring maps
  map<TierId, GlobalHashRing> global_hash_rings;
  map<TierId, LocalHashRing> local_hash_rings;

  // for periodically redistributing data when node joins
  AddressKeysetMap join_gossip_map;

  // keep track of which key should be removed when node joins
  set<Key> join_remove_set;

  // for tracking IP addresses of extant caches
  set<Address> extant_caches;
  // TODO: actually populate

  // For tracking the keys each extant cache is responsible for.
  // This is just our thread's cache of this.
  map<Address, set<Key>> cache_ip_to_keys;
  // TODO: update with updates to extant_caches (only deletions of caches matter)

  // For tracking the caches that hold a given key.
  // Inverse of cache_ip_to_keys.
  // We need the two structures because
  // key->caches is the one necessary for gossiping upon key updates,
  // but the mapping is provided to us in the form cache->keys,
  // so we need a local copy of this mapping in order to update key->caches
  // with dropped keys when we receive a fresh cache->keys record.
  map<Key, set<Address>> key_to_cache_ips;

  // pending events for asynchrony
  map<Key, vector<PendingRequest>> pending_requests;
  map<Key, vector<PendingGossip>> pending_gossip;

  map<Key, KeyMetadata> metadata_map;

  // request server addresses from the seed node
  zmq::socket_t addr_requester(context, ZMQ_REQ);
  addr_requester.connect(RoutingThread(seed_ip, 0).seed_connect_address());
  kZmqUtil->send_string("join", &addr_requester);

  // receive and add all the addresses that seed node sent
  string serialized_addresses = kZmqUtil->recv_string(&addr_requester);
  TierMembership membership;
  membership.ParseFromString(serialized_addresses);

  // get join number from management node if we are running in Kubernetes
  string count_str;

  // if we are running the system outside of Kubernetes, we need to set the
  // management address to NULL in the conf file, otherwise we will hang
  // forever waiting to hear back about a restart count
  if (management_ip != "NULL") {
    zmq::socket_t join_count_requester(context, ZMQ_REQ);
    join_count_requester.connect(get_join_count_req_address(management_ip));
    kZmqUtil->send_string("restart:" + private_ip, &join_count_requester);
    count_str = kZmqUtil->recv_string(&join_count_requester);
  } else {
    count_str = "0";
  }

  int self_join_count = stoi(count_str);

  // populate addresses
  for (const auto& tier : membership.tiers()) {
    for (const auto server : tier.servers()) {
      global_hash_rings[tier.tier_id()].insert(server.public_ip(),
                                               server.private_ip(), 0, 0);
    }
  }

  // add itself to global hash ring
  global_hash_rings[kSelfTierId].insert(public_ip, private_ip, self_join_count,
                                        0);

  // form local hash rings
  for (const auto& pair : kTierMetadata) {
    TierMetadata tier = pair.second;
    for (unsigned tid = 0; tid < tier.thread_number_; tid++) {
      local_hash_rings[tier.id_].insert(public_ip, private_ip, 0, tid);
    }
  }

  // thread 0 notifies other servers that it has joined
  if (thread_id == 0) {
    string msg = std::to_string(kSelfTierId) + ":" + public_ip + ":" +
                 private_ip + ":" + count_str;

    for (const auto& pair : global_hash_rings) {
      GlobalHashRing hash_ring = pair.second;

      for (const ServerThread& st : hash_ring.get_unique_servers()) {
        if (st.private_ip().compare(private_ip) != 0) {
          kZmqUtil->send_string(msg, &pushers[st.node_join_connect_address()]);
        }
      }
    }

    msg = "join:" + msg;

    // notify proxies that this node has joined
    for (const string& address : routing_ips) {
      kZmqUtil->send_string(
          msg, &pushers[RoutingThread(address, 0).notify_connect_address()]);
    }

    // notify monitoring nodes that this node has joined
    for (const string& address : monitoring_ips) {
      kZmqUtil->send_string(
          msg, &pushers[MonitoringThread(address).notify_connect_address()]);
    }
  }

  SerializerMap serializers;

  Serializer* lww_serializer;
  Serializer* set_serializer;
  Serializer* causal_serializer;

  if (kSelfTierId == kMemoryTierId) {
    MemoryLWWKVS* lww_kvs = new MemoryLWWKVS();
    lww_serializer = new MemoryLWWSerializer(lww_kvs);
    MemorySetKVS* set_kvs = new MemorySetKVS();
    set_serializer = new MemorySetSerializer(set_kvs);
    MemoryCausalKVS* causal_kvs = new MemoryCausalKVS();
    causal_serializer = new MemoryCausalSerializer(causal_kvs);
  } else if (kSelfTierId == kEbsTierId) {
    lww_serializer = new EBSLWWSerializer(thread_id);
    set_serializer = new EBSSetSerializer(thread_id);
    causal_serializer = new EBSCausalSerializer(thread_id);
  } else {
    log->info("Invalid node type");
    exit(1);
  }

  serializers[LatticeType::LWW] = lww_serializer;
  serializers[LatticeType::SET] = set_serializer;
  serializers[LatticeType::CAUSAL] = causal_serializer;

  // the set of changes made on this thread since the last round of gossip
  set<Key> local_changeset;

  // keep track of the key stat
  // the first entry is the size of the key,
  // the second entry is its lattice type.
  // keep track of key access timestamp
  map<Key, std::multiset<TimePoint>> key_access_tracker;
  // keep track of total access
  unsigned access_count;

  // listens for a new node joining
  zmq::socket_t join_puller(context, ZMQ_PULL);
  join_puller.bind(wt.node_join_bind_address());

  // listens for a node departing
  zmq::socket_t depart_puller(context, ZMQ_PULL);
  depart_puller.bind(wt.node_depart_bind_address());

  // responsible for listening for a command that this node should leave
  zmq::socket_t self_depart_puller(context, ZMQ_PULL);
  self_depart_puller.bind(wt.self_depart_bind_address());

  // responsible for handling requests
  zmq::socket_t request_puller(context, ZMQ_PULL);
  request_puller.bind(wt.key_request_bind_address());

  // responsible for processing gossip
  zmq::socket_t gossip_puller(context, ZMQ_PULL);
  gossip_puller.bind(wt.gossip_bind_address());

  // responsible for listening for key replication factor response
  zmq::socket_t replication_response_puller(context, ZMQ_PULL);
  replication_response_puller.bind(wt.replication_response_bind_address());

  // responsible for listening for key replication factor change
  zmq::socket_t replication_change_puller(context, ZMQ_PULL);
  replication_change_puller.bind(wt.replication_change_bind_address());

  // responsible for listening for cache IP lookup response messages.
  zmq::socket_t cache_ip_response_puller(context, ZMQ_PULL);
  cache_ip_response_puller.bind(wt.cache_ip_response_bind_address());

  //  Initialize poll set
  vector<zmq::pollitem_t> pollitems = {
      {static_cast<void*>(join_puller), 0, ZMQ_POLLIN, 0},
      {static_cast<void*>(depart_puller), 0, ZMQ_POLLIN, 0},
      {static_cast<void*>(self_depart_puller), 0, ZMQ_POLLIN, 0},
      {static_cast<void*>(request_puller), 0, ZMQ_POLLIN, 0},
      {static_cast<void*>(gossip_puller), 0, ZMQ_POLLIN, 0},
      {static_cast<void*>(replication_response_puller), 0, ZMQ_POLLIN, 0},
      {static_cast<void*>(replication_change_puller), 0, ZMQ_POLLIN, 0},
      {static_cast<void*>(cache_ip_response_puller), 0, ZMQ_POLLIN, 0}};

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

      string serialized = kZmqUtil->recv_string(&join_puller);
      node_join_handler(thread_id, seed, public_ip, private_ip, log, serialized,
                        global_hash_rings, local_hash_rings, metadata_map,
                        join_remove_set, pushers, wt, join_gossip_map,
                        self_join_count);

      auto time_elapsed = std::chrono::duration_cast<std::chrono::microseconds>(
                              std::chrono::system_clock::now() - work_start)
                              .count();
      working_time += time_elapsed;
      working_time_map[0] += time_elapsed;
    }

    if (pollitems[1].revents & ZMQ_POLLIN) {
      auto work_start = std::chrono::system_clock::now();

      string serialized = kZmqUtil->recv_string(&depart_puller);
      node_depart_handler(thread_id, public_ip, private_ip, global_hash_rings,
                          log, serialized, pushers);

      auto time_elapsed = std::chrono::duration_cast<std::chrono::microseconds>(
                              std::chrono::system_clock::now() - work_start)
                              .count();
      working_time += time_elapsed;
      working_time_map[1] += time_elapsed;
    }

    if (pollitems[2].revents & ZMQ_POLLIN) {
      string serialized = kZmqUtil->recv_string(&self_depart_puller);
      self_depart_handler(thread_id, seed, public_ip, private_ip, log,
                          serialized, global_hash_rings, local_hash_rings,
                          metadata_map, routing_ips, monitoring_ips, wt,
                          pushers, serializers);

      return;
    }

    if (pollitems[3].revents & ZMQ_POLLIN) {
      auto work_start = std::chrono::system_clock::now();

      string serialized = kZmqUtil->recv_string(&request_puller);
      user_request_handler(access_count, seed, serialized, log,
                           global_hash_rings, local_hash_rings,
                           pending_requests, key_access_tracker, metadata_map,
                           local_changeset, wt, serializers, pushers);

      auto time_elapsed = std::chrono::duration_cast<std::chrono::microseconds>(
                              std::chrono::system_clock::now() - work_start)
                              .count();
      working_time += time_elapsed;
      working_time_map[3] += time_elapsed;
    }

    // receive gossip
    if (pollitems[4].revents & ZMQ_POLLIN) {
      auto work_start = std::chrono::system_clock::now();

      string serialized = kZmqUtil->recv_string(&gossip_puller);
      gossip_handler(seed, serialized, global_hash_rings, local_hash_rings,
                     pending_gossip, metadata_map, wt, serializers, pushers,
                     log);

      auto time_elapsed = std::chrono::duration_cast<std::chrono::microseconds>(
                              std::chrono::system_clock::now() - work_start)
                              .count();
      working_time += time_elapsed;
      working_time_map[4] += time_elapsed;
    }

    // receives replication factor response
    if (pollitems[5].revents & ZMQ_POLLIN) {
      auto work_start = std::chrono::system_clock::now();

      string serialized = kZmqUtil->recv_string(&replication_response_puller);
      replication_response_handler(seed, access_count, log, serialized,
                                   global_hash_rings, local_hash_rings,
                                   pending_requests, pending_gossip,
                                   key_access_tracker, metadata_map,
                                   local_changeset, wt, serializers, pushers);

      auto time_elapsed = std::chrono::duration_cast<std::chrono::microseconds>(
                              std::chrono::system_clock::now() - work_start)
                              .count();
      working_time += time_elapsed;
      working_time_map[5] += time_elapsed;
    }

    // receive replication factor change
    if (pollitems[6].revents & ZMQ_POLLIN) {
      auto work_start = std::chrono::system_clock::now();

      string serialized = kZmqUtil->recv_string(&replication_change_puller);
      replication_change_handler(public_ip, private_ip, thread_id, seed, log,
                                 serialized, global_hash_rings,
                                 local_hash_rings, metadata_map,
                                 local_changeset, wt, serializers, pushers);

      auto time_elapsed = std::chrono::duration_cast<std::chrono::microseconds>(
                              std::chrono::system_clock::now() - work_start)
                              .count();
      working_time += time_elapsed;
      working_time_map[6] += time_elapsed;
    }

    // Receive cache IP lookup response.
    if (pollitems[7].revents & ZMQ_POLLIN) {
      auto work_start = std::chrono::system_clock::now();

      string serialized = kZmqUtil->recv_string(&cache_ip_response_puller);
      cache_ip_response_handler(serialized, cache_ip_to_keys, key_to_cache_ips);

      auto time_elapsed = std::chrono::duration_cast<std::chrono::microseconds>(
                              std::chrono::system_clock::now() - work_start)
                              .count();
      working_time += time_elapsed;
      working_time_map[7] += time_elapsed;
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
          // Get the threads that we need to gossip to.
          ServerThreadList threads = kHashRingUtil->get_responsible_threads(
              wt.replication_response_connect_address(), key, is_metadata(key),
              global_hash_rings, local_hash_rings, metadata_map, pushers,
              kAllTierIds, succeed, seed);

          if (succeed) {
            for (const ServerThread& thread : threads) {
              addr_keyset_map[thread.gossip_connect_address()].insert(key);
            }
          } else {
            log->error("Missing key replication factor in gossip routine.");
          }

          // Get the caches that we need to gossip to.
          set<Address>& cache_ips = key_to_cache_ips[key];
          for (const Address& cache_ip : cache_ips) {
            // XXX TODO
            // cache_ip here doesn't have a port and needs it added,
            // probably through gossip_connect_address() of CacheThread
            // once that's implemented.
            addr_keyset_map[cache_ip].insert(key);
          }
        }

        send_gossip(addr_keyset_map, pushers, serializers, metadata_map);
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
    // Also, send out GET requests for the cached keys by cache IP.
    report_end = std::chrono::system_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::seconds>(
                        report_end - report_start)
                        .count();

    if (duration >= kServerReportThreshold) {
      epoch += 1;
      auto ts = generate_timestamp(wt.tid());

      Key key = get_metadata_key(wt, kSelfTierId, wt.tid(),
                                 MetadataType::server_stats);

      // compute total storage consumption
      unsigned long long consumption = 0;
      for (const auto& key_pair : metadata_map) {
        consumption += key_pair.second.size_;
      }

      int index = 0;
      for (const unsigned long long& time : working_time_map) {
        // cast to microsecond
        double event_occupancy = (double)time / ((double)duration * 1000000);

        if (event_occupancy > 0.02) {
          log->info("Event {} occupancy is {}.", std::to_string(index++),
                    std::to_string(event_occupancy));
        }
      }

      double occupancy = (double)working_time / ((double)duration * 1000000);
      if (occupancy > 0.02) {
        log->info("Occupancy is {}.", std::to_string(occupancy));
      }

      ServerThreadStatistics stat;
      stat.set_storage_consumption(consumption / 1000);  // cast to KB
      stat.set_occupancy(occupancy);
      stat.set_epoch(epoch);
      stat.set_access_count(access_count);

      string serialized_stat;
      stat.SerializeToString(&serialized_stat);

      KeyRequest req;
      req.set_type(RequestType::PUT);
      prepare_put_tuple(req, key, LatticeType::LWW,
                        serialize(ts, serialized_stat));

      auto threads = kHashRingUtil->get_responsible_threads_metadata(
          key, global_hash_rings[kMemoryTierId],
          local_hash_rings[kMemoryTierId]);
      if (threads.size() != 0) {
        Address target_address =
            std::next(begin(threads), rand_r(&seed) % threads.size())
                ->key_request_connect_address();
        string serialized;
        req.SerializeToString(&serialized);
        kZmqUtil->send_string(serialized, &pushers[target_address]);
      }

      // compute key access stats
      KeyAccessData access;
      auto current_time = std::chrono::system_clock::now();

      for (const auto& key_access_pair : key_access_tracker) {
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
      key =
          get_metadata_key(wt, kSelfTierId, wt.tid(), MetadataType::key_access);
      string serialized_access;
      access.SerializeToString(&serialized_access);

      req.Clear();
      req.set_type(RequestType::PUT);
      prepare_put_tuple(req, key, LatticeType::LWW,
                        serialize(ts, serialized_access));

      threads = kHashRingUtil->get_responsible_threads_metadata(
          key, global_hash_rings[kMemoryTierId],
          local_hash_rings[kMemoryTierId]);

      if (threads.size() != 0) {
        Address target_address =
            std::next(begin(threads), rand_r(&seed) % threads.size())
                ->key_request_connect_address();
        string serialized;
        req.SerializeToString(&serialized);
        kZmqUtil->send_string(serialized, &pushers[target_address]);
      }

      // report key size stats
      KeySizeData primary_key_size;
      for (const auto& key_pair : metadata_map) {
        if (is_primary_replica(key_pair.first, metadata_map, global_hash_rings,
                               local_hash_rings, wt)) {
          KeySizeData_KeySize* ks = primary_key_size.add_key_sizes();
          ks->set_key(key_pair.first);
          ks->set_size(key_pair.second.size_);
        }
      }

      key = get_metadata_key(wt, kSelfTierId, wt.tid(), MetadataType::key_size);

      string serialized_size;
      primary_key_size.SerializeToString(&serialized_size);

      req.Clear();
      req.set_type(RequestType::PUT);
      prepare_put_tuple(req, key, LatticeType::LWW,
                        serialize(ts, serialized_size));

      threads = kHashRingUtil->get_responsible_threads_metadata(
          key, global_hash_rings[kMemoryTierId],
          local_hash_rings[kMemoryTierId]);

      if (threads.size() != 0) {
        Address target_address =
            std::next(begin(threads), rand_r(&seed) % threads.size())
                ->key_request_connect_address();
        string serialized;
        req.SerializeToString(&serialized);
        kZmqUtil->send_string(serialized, &pushers[target_address]);
      }

      report_start = std::chrono::system_clock::now();

      // Get the cached keys by cache IP.
      // First, prepare the requests for all the IPs we know about
      // and put them in an address request map.
      map<Address, KeyRequest> addr_request_map;
      for (const auto& cacheip : extant_caches) {
        Key key = get_metadata_key(cacheip, MetadataType::cache_ip);
        prepare_metadata_get_request(
          key, global_hash_rings[kMemoryTierId], local_hash_rings[kMemoryTierId],
          addr_request_map, wt.cache_ip_response_connect_address(), rid);
      }

      // Loop over the address request map and execute all the requests.
      for (const auto& addr_request : addr_request_map) {
        send_request<KeyRequest>(addr_request.second, pushers[addr_request.first]);
      }

      // reset stats tracked in memory
      working_time = 0;
      access_count = 0;
      memset(working_time_map, 0, sizeof(working_time_map));
    }

    // redistribute data after node joins
    if (join_gossip_map.size() != 0) {
      set<Address> remove_address_set;

      // assemble gossip
      AddressKeysetMap addr_keyset_map;
      for (const auto& join_pair : join_gossip_map) {
        Address address = join_pair.first;
        set<Key> key_set = join_pair.second;
        // track all sent keys because we cannot modify the key_set while
        // iterating over it
        set<Key> sent_keys;

        for (const Key& key : key_set) {
          addr_keyset_map[address].insert(key);
          sent_keys.insert(key);
          if (sent_keys.size() >= DATA_REDISTRIBUTE_THRESHOLD) {
            break;
          }
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
        join_gossip_map.erase(remove_address);
      }

      send_gossip(addr_keyset_map, pushers, serializers, metadata_map);

      // remove keys
      if (join_gossip_map.size() == 0) {
        for (const string& key : join_remove_set) {
          serializers[metadata_map[key].type_]->remove(key);
          metadata_map.erase(key);
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
    if (strncmp(stype, "memory", 6) == 0) {
      kSelfTierId = kMemoryTierId;
    } else if (strncmp(stype, "ebs", 3) == 0) {
      kSelfTierId = kEbsTierId;
    } else {
      std::cout << "Unrecognized server type " << stype
                << ". Valid types are memory or ebs." << std::endl;
      return 1;
    }
  } else {
    std::cout
        << "No server type specified. The default behavior is to start the "
           "server in memory mode."
        << std::endl;
    kSelfTierId = kMemoryTierId;
  }

  kSelfTierIdVector = {kSelfTierId};

  // read the YAML conf
  YAML::Node conf = YAML::LoadFile("conf/kvs-config.yml");
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

  YAML::Node server = conf["server"];
  Address public_ip = server["public_ip"].as<string>();
  Address private_ip = server["private_ip"].as<string>();

  vector<Address> routing_ips;
  vector<Address> monitoring_ips;

  Address seed_ip = server["seed_ip"].as<string>();
  Address mgmt_ip = server["mgmt_ip"].as<string>();
  YAML::Node monitoring = server["monitoring"];
  YAML::Node routing = server["routing"];

  for (const YAML::Node& address : routing) {
    routing_ips.push_back(address.as<Address>());
  }

  for (const YAML::Node& address : monitoring) {
    monitoring_ips.push_back(address.as<Address>());
  }

  kTierMetadata[kMemoryTierId] =
      TierMetadata(kMemoryTierId, kMemoryThreadCount,
                   kDefaultGlobalMemoryReplication, kMemoryNodeCapacity);
  kTierMetadata[kEbsTierId] =
      TierMetadata(kEbsTierId, kEbsThreadCount, kDefaultGlobalEbsReplication,
                   kEbsNodeCapacity);

  kThreadNum = kTierMetadata[kSelfTierId].thread_number_;

  // start the initial threads based on kThreadNum
  vector<std::thread> worker_threads;
  for (unsigned thread_id = 1; thread_id < kThreadNum; thread_id++) {
    worker_threads.push_back(std::thread(run, thread_id, public_ip, private_ip,
                                         seed_ip, routing_ips, monitoring_ips,
                                         mgmt_ip));
  }

  run(0, public_ip, private_ip, seed_ip, routing_ips, monitoring_ips, mgmt_ip);

  // join on all threads to make sure they finish before exiting
  for (unsigned tid = 1; tid < kThreadNum; tid++) {
    worker_threads[tid].join();
  }

  return 0;
}
