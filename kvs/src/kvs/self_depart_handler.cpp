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
                         Address private_ip, logger log, string& serialized,
                         map<TierId, GlobalHashRing>& global_hash_rings,
                         map<TierId, LocalHashRing>& local_hash_rings,
                         map<Key, KeyProperty>& stored_key_map,
                         map<Key, KeyReplication>& key_replication_map,
                         vector<Address>& routing_ips,
                         vector<Address>& monitoring_ips, ServerThread& wt,
                         SocketCache& pushers, SerializerMap& serializers) {
  log->info("Node is departing.");
  global_hash_rings[kSelfTierId].remove(public_ip, private_ip, 0);

  // thread 0 notifies other nodes in the cluster (of all types) that it is
  // leaving the cluster
  if (thread_id == 0) {
    string msg =
        std::to_string(kSelfTierId) + ":" + public_ip + ":" + private_ip;

    for (const auto& pair : global_hash_rings) {
      GlobalHashRing hash_ring = pair.second;

      for (const ServerThread& st : hash_ring.get_unique_servers()) {
        kZmqUtil->send_string(msg, &pushers[st.node_depart_connect_address()]);
      }
    }

    msg = "depart:" + msg;

    // notify all routing nodes
    for (const string& address : routing_ips) {
      kZmqUtil->send_string(
          msg, &pushers[RoutingThread(address, 0).notify_connect_address()]);
    }

    // notify monitoring nodes
    for (const string& address : monitoring_ips) {
      kZmqUtil->send_string(
          msg, &pushers[MonitoringThread(address).notify_connect_address()]);
    }

    // tell all worker threads about the self departure
    for (unsigned tid = 1; tid < kThreadNum; tid++) {
      kZmqUtil->send_string(serialized,
                            &pushers[ServerThread(public_ip, private_ip, tid)
                                         .self_depart_connect_address()]);
    }
  }

  AddressKeysetMap addr_keyset_map;
  bool succeed;

  for (const auto& key_pair : stored_key_map) {
    Key key = key_pair.first;
    ServerThreadList threads = kHashRingUtil->get_responsible_threads(
        wt.replication_response_connect_address(), key, is_metadata(key),
        global_hash_rings, local_hash_rings, key_replication_map, pushers,
        kAllTierIds, succeed, seed);

    if (succeed) {
      // since we already removed this node from the hash ring, no need to
      // exclude it explicitly
      for (const ServerThread& thread : threads) {
        addr_keyset_map[thread.gossip_connect_address()].insert(key);
      }
    } else {
      log->error("Missing key replication factor in node depart routine");
    }
  }

  send_gossip(addr_keyset_map, pushers, serializers, stored_key_map);
  kZmqUtil->send_string(
      public_ip + "_" + private_ip + "_" + std::to_string(kSelfTierId),
      &pushers[serialized]);
}
