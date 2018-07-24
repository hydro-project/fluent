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

void self_depart_handler(
    unsigned thread_id, unsigned& seed, Address ip,
    std::shared_ptr<spdlog::logger> logger, std::string& serialized,
    std::unordered_map<unsigned, GlobalHashRing>& global_hash_ring_map,
    std::unordered_map<unsigned, LocalHashRing>& local_hash_ring_map,
    std::unordered_map<Key, unsigned>& key_size_map,
    std::unordered_map<Key, KeyInfo>& placement,
    std::vector<Address>& routing_address,
    std::vector<Address>& monitoring_address, ServerThread& wt,
    SocketCache& pushers, Serializer* serializer) {
  logger->info("Node is departing.");
  global_hash_ring_map[kSelfTierId].remove(ip, 0);

  // thread 0 notifies other nodes in the cluster (of all types) that it is
  // leaving the cluster
  if (thread_id == 0) {
    std::string msg = std::to_string(kSelfTierId) + ":" + ip;

    for (const auto& global_pair : global_hash_ring_map) {
      GlobalHashRing hash_ring = global_pair.second;

      for (const ServerThread& st : hash_ring.get_unique_servers()) {
        kZmqUtil->send_string(msg, &pushers[st.get_node_depart_connect_addr()]);
      }
    }

    msg = "depart:" + std::to_string(kSelfTierId) + ":" + ip;

    // notify all routing nodes
    for (const std::string& address : routing_address) {
      kZmqUtil->send_string(
          msg, &pushers[RoutingThread(address, 0).get_notify_connect_addr()]);
    }

    // notify monitoring nodes
    for (const std::string& address : monitoring_address) {
      kZmqUtil->send_string(
          msg, &pushers[MonitoringThread(address).get_notify_connect_addr()]);
    }

    // tell all worker threads about the self departure
    for (unsigned tid = 1; tid < kThreadNum; tid++) {
      kZmqUtil->send_string(
          serialized,
          &pushers[ServerThread(ip, tid).get_self_depart_connect_addr()]);
    }
  }

  AddressKeysetMap addr_keyset_map;
  bool succeed;

  for (const auto& key_pair : key_size_map) {
    Key key = key_pair.first;
    ServerThreadSet threads = kHashRingUtil->get_responsible_threads(
        wt.get_replication_factor_connect_addr(), key, is_metadata(key),
        global_hash_ring_map, local_hash_ring_map, placement, pushers,
        kAllTierIds, succeed, seed);

    if (succeed) {
      // since we already removed this node from the hash ring, no need to
      // exclude it explicitly
      for (const ServerThread& thread : threads) {
        addr_keyset_map[thread.get_gossip_connect_addr()].insert(key);
      }
    } else {
      logger->error("Missing key replication factor in node depart routine");
    }
  }

  send_gossip(addr_keyset_map, pushers, serializer);
  kZmqUtil->send_string(ip + "_" + std::to_string(kSelfTierId),
                        &pushers[serialized]);
}
