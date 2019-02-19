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

void self_depart_handler(unsigned thread_id, unsigned& seed, Address public_ip,
                         Address private_ip,
                         std::shared_ptr<spdlog::logger> logger,
                         string& serialized,
                         map<TierId, GlobalHashRing>& global_hash_rings,
                         map<TierId, LocalHashRing>& local_hash_rings,
                         map<Key, KeyMetadata>& metadata_map,
                         vector<Address>& routing_address,
                         vector<Address>& monitoring_address, ServerThread& wt,
                         SocketCache& pushers, SerializerMap& serializers) {
  logger->info("Node is departing.");
  global_hash_rings[kSelfTierId].remove(public_ip, private_ip, 0);

  // thread 0 notifies other nodes in the cluster (of all types) that it is
  // leaving the cluster
  if (thread_id == 0) {
    string msg =
        std::to_string(kSelfTierId) + ":" + public_ip + ":" + private_ip;

    for (const auto& pair : global_hash_rings) {
      GlobalHashRing hash_ring = pair.second;

      for (const ServerThread& st : hash_ring.get_unique_servers()) {
        kZmqUtil->send_string(msg, &pushers[st.get_node_depart_connect_addr()]);
      }
    }

    msg = "depart:" + msg;

    // notify all routing nodes
    for (const string& address : routing_address) {
      kZmqUtil->send_string(
          msg, &pushers[RoutingThread(address, 0).get_notify_connect_addr()]);
    }

    // notify monitoring nodes
    for (const string& address : monitoring_address) {
      kZmqUtil->send_string(
          msg, &pushers[MonitoringThread(address).get_notify_connect_addr()]);
    }

    // tell all worker threads about the self departure
    for (unsigned tid = 1; tid < kThreadNum; tid++) {
      kZmqUtil->send_string(serialized,
                            &pushers[ServerThread(public_ip, private_ip, tid)
                                         .get_self_depart_connect_addr()]);
    }
  }

  AddressKeysetMap addr_keyset_map;
  bool succeed;

  for (const auto& key_pair : metadata_map) {
    Key key = key_pair.first;
    ServerThreadList threads = kHashRingUtil->get_responsible_threads(
        wt.get_replication_factor_connect_addr(), key, is_metadata(key),
        global_hash_rings, local_hash_rings, metadata_map, pushers, kAllTierIds,
        succeed, seed);

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

  send_gossip(addr_keyset_map, pushers, serializers, metadata_map);
  kZmqUtil->send_string(
      public_ip + "_" + private_ip + "_" + std::to_string(kSelfTierId),
      &pushers[serialized]);
}
