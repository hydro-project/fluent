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
    std::unordered_map<Key, unsigned>& key_size_map,
    PendingMap<PendingGossip>& pending_gossip_map,
    std::unordered_map<Key, KeyInfo>& placement, ServerThread& wt,
    Serializer* serializer, SocketCache& pushers) {
  KeyRequest gossip;
  gossip.ParseFromString(serialized);

  bool succeed;
  std::unordered_map<Address, KeyRequest> gossip_map;

  for (const KeyTuple& tuple : gossip.tuples()) {
    // first check if the thread is responsible for the key
    Key key = tuple.key();
    ServerThreadSet threads = kHashRingUtil->get_responsible_threads(
        wt.get_replication_factor_connect_addr(), key, is_metadata(key),
        global_hash_ring_map, local_hash_ring_map, placement, pushers,
        kSelfTierIdVector, succeed, seed);

    if (succeed) {
      if (threads.find(wt) !=
          threads.end()) {  // this means this worker thread is one of the
                            // responsible threads
        process_put(tuple.key(), tuple.timestamp(), tuple.value(), serializer,
                    key_size_map);
      } else {
        if (is_metadata(key)) {  // forward the gossip
          for (const ServerThread& thread : threads) {
            if (gossip_map.find(thread.get_gossip_connect_addr()) ==
                gossip_map.end()) {
              gossip_map[thread.get_gossip_connect_addr()].set_type(
                  get_request_type("PUT"));
            }

            prepare_put_tuple(gossip_map[thread.get_gossip_connect_addr()], key,
                              tuple.value(), tuple.timestamp());
          }
        } else {
          kHashRingUtil->issue_replication_factor_request(
              wt.get_replication_factor_connect_addr(), key,
              global_hash_ring_map[1], local_hash_ring_map[1], pushers, seed);

          pending_gossip_map[key].push_back(
              PendingGossip(tuple.value(), tuple.timestamp()));
        }
      }
    } else {
      pending_gossip_map[key].push_back(
          PendingGossip(tuple.value(), tuple.timestamp()));
    }
  }

  // redirect gossip
  for (const auto& gossip_pair : gossip_map) {
    std::string serialized;
    gossip_pair.second.SerializeToString(&serialized);
    kZmqUtil->send_string(serialized, &pushers[gossip_pair.first]);
  }
}
