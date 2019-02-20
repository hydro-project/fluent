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

void gossip_handler(
    unsigned& seed, std::string& serialized,
    std::unordered_map<unsigned, GlobalHashRing>& global_hash_ring_map,
    std::unordered_map<unsigned, LocalHashRing>& local_hash_ring_map,
    std::unordered_map<Key, std::pair<unsigned, LatticeType>>& key_stat_map,
    PendingMap<PendingGossip>& pending_gossip_map,
    std::unordered_map<Key, KeyInfo>& placement, ServerThread& wt,
    std::unordered_map<LatticeType, Serializer*, lattice_type_hash>&
        serializers,
    SocketCache& pushers, std::shared_ptr<spdlog::logger> logger) {
  KeyRequest gossip;
  gossip.ParseFromString(serialized);

  bool succeed;
  std::unordered_map<Address, KeyRequest> gossip_map;

  for (const KeyTuple& tuple : gossip.tuples()) {
    // first check if the thread is responsible for the key
    Key key = tuple.key();
    ServerThreadList threads = kHashRingUtil->get_responsible_threads(
        wt.get_replication_factor_connect_addr(), key, is_metadata(key),
        global_hash_ring_map, local_hash_ring_map, placement, pushers,
        kSelfTierIdVector, succeed, seed);

    if (succeed) {
      if (std::find(threads.begin(), threads.end(), wt) !=
          threads.end()) {  // this means this worker thread is one of the
                            // responsible threads
        if (key_stat_map.find(key) != key_stat_map.end() &&
            key_stat_map[key].second != tuple.lattice_type()) {
          logger->error("Lattice type mismatch: {} from query but {} expected.",
                        LatticeType_Name(tuple.lattice_type()),
                        key_stat_map[key].second);
        } else {
          process_put(tuple.key(), tuple.lattice_type(), tuple.payload(),
                      serializers[tuple.lattice_type()], key_stat_map);
        }
      } else {
        if (is_metadata(key)) {  // forward the gossip
          for (const ServerThread& thread : threads) {
            if (gossip_map.find(thread.get_gossip_connect_addr()) ==
                gossip_map.end()) {
              gossip_map[thread.get_gossip_connect_addr()].set_type(
                  RequestType::PUT);
            }

            prepare_put_tuple(gossip_map[thread.get_gossip_connect_addr()], key,
                              tuple.lattice_type(), tuple.payload());
          }
        } else {
          kHashRingUtil->issue_replication_factor_request(
              wt.get_replication_factor_connect_addr(), key,
              global_hash_ring_map[1], local_hash_ring_map[1], pushers, seed);

          pending_gossip_map[key].push_back(
              PendingGossip(tuple.lattice_type(), tuple.payload()));
        }
      }
    } else {
      pending_gossip_map[key].push_back(
          PendingGossip(tuple.lattice_type(), tuple.payload()));
    }
  }

  // redirect gossip
  for (const auto& gossip_pair : gossip_map) {
    std::string serialized;
    gossip_pair.second.SerializeToString(&serialized);
    kZmqUtil->send_string(serialized, &pushers[gossip_pair.first]);
  }
}
