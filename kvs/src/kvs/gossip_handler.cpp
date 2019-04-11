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

void gossip_handler(unsigned& seed, string& serialized,
                    map<TierId, GlobalHashRing>& global_hash_rings,
                    map<TierId, LocalHashRing>& local_hash_rings,
                    map<Key, vector<PendingGossip>>& pending_gossip,
                    map<Key, KeyProperty>& stored_key_map,
                    map<Key, KeyReplication>& key_replication_map,
                    ServerThread& wt, SerializerMap& serializers,
                    SocketCache& pushers, logger log) {
  KeyRequest gossip;
  gossip.ParseFromString(serialized);

  bool succeed;
  map<Address, KeyRequest> gossip_map;

  for (const KeyTuple& tuple : gossip.tuples()) {
    // first check if the thread is responsible for the key
    Key key = tuple.key();
    ServerThreadList threads = kHashRingUtil->get_responsible_threads(
        wt.replication_response_connect_address(), key, is_metadata(key),
        global_hash_rings, local_hash_rings, key_replication_map, pushers,
        kSelfTierIdVector, succeed, seed);

    if (succeed) {
      if (std::find(threads.begin(), threads.end(), wt) !=
          threads.end()) {  // this means this worker thread is one of the
                            // responsible threads
        if (stored_key_map.find(key) != stored_key_map.end() &&
            stored_key_map[key].type_ != tuple.lattice_type()) {
          log->error("Lattice type mismatch: {} from query but {} expected.",
                     LatticeType_Name(tuple.lattice_type()),
                     stored_key_map[key].type_);
        } else {
          process_put(tuple.key(), tuple.lattice_type(), tuple.payload(),
                      serializers[tuple.lattice_type()], stored_key_map);
        }
      } else {
        if (is_metadata(key)) {  // forward the gossip
          for (const ServerThread& thread : threads) {
            if (gossip_map.find(thread.gossip_connect_address()) ==
                gossip_map.end()) {
              gossip_map[thread.gossip_connect_address()].set_type(
                  RequestType::PUT);
            }

            prepare_put_tuple(gossip_map[thread.gossip_connect_address()], key,
                              tuple.lattice_type(), tuple.payload());
          }
        } else {
          kHashRingUtil->issue_replication_factor_request(
              wt.replication_response_connect_address(), key,
              global_hash_rings[kMemoryTierId], local_hash_rings[kMemoryTierId],
              pushers, seed);

          pending_gossip[key].push_back(
              PendingGossip(tuple.lattice_type(), tuple.payload()));
        }
      }
    } else {
      pending_gossip[key].push_back(
          PendingGossip(tuple.lattice_type(), tuple.payload()));
    }
  }

  // redirect gossip
  for (const auto& gossip_pair : gossip_map) {
    string serialized;
    gossip_pair.second.SerializeToString(&serialized);
    kZmqUtil->send_string(serialized, &pushers[gossip_pair.first]);
  }
}
