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

void replication_change_handler(Address public_ip, Address private_ip,
                                unsigned thread_id, unsigned& seed, logger log,
                                string& serialized,
                                map<TierId, GlobalHashRing>& global_hash_rings,
                                map<TierId, LocalHashRing>& local_hash_rings,
                                map<Key, KeyProperty>& stored_key_map,
                                map<Key, KeyReplication>& key_replication_map,
                                set<Key>& local_changeset, ServerThread& wt,
                                SerializerMap& serializers,
                                SocketCache& pushers) {
  log->info("Received a replication factor change.");
  if (thread_id == 0) {
    // tell all worker threads about the replication factor change
    for (unsigned tid = 1; tid < kThreadNum; tid++) {
      kZmqUtil->send_string(
          serialized, &pushers[ServerThread(public_ip, private_ip, tid)
                                   .replication_change_connect_address()]);
    }
  }

  ReplicationFactorUpdate rep_change;
  rep_change.ParseFromString(serialized);

  AddressKeysetMap addr_keyset_map;
  set<Key> remove_set;

  // for every key, update the replication factor and check if the node is still
  // responsible for the key
  bool succeed;

  for (const ReplicationFactor& key_rep : rep_change.key_reps()) {
    Key key = key_rep.key();
    // if this thread has the key stored before the change
    if (stored_key_map.find(key) != stored_key_map.end()) {
      ServerThreadList orig_threads = kHashRingUtil->get_responsible_threads(
          wt.replication_response_connect_address(), key, is_metadata(key),
          global_hash_rings, local_hash_rings, key_replication_map, pushers,
          kAllTierIds, succeed, seed);

      if (succeed) {
        // update the replication factor
        bool decrement = false;

        for (const auto& global : key_rep.global()) {
          if (global.replication_factor() <
              key_replication_map[key].global_replication_[global.tier_id()]) {
            decrement = true;
          }

          key_replication_map[key].global_replication_[global.tier_id()] =
              global.replication_factor();
        }

        for (const auto& local : key_rep.local()) {
          if (local.replication_factor() <
              key_replication_map[key].local_replication_[local.tier_id()]) {
            decrement = true;
          }

          key_replication_map[key].local_replication_[local.tier_id()] =
              local.replication_factor();
        }

        ServerThreadList threads = kHashRingUtil->get_responsible_threads(
            wt.replication_response_connect_address(), key, is_metadata(key),
            global_hash_rings, local_hash_rings, key_replication_map, pushers,
            kAllTierIds, succeed, seed);

        if (succeed) {
          if (std::find(threads.begin(), threads.end(), wt) ==
              threads.end()) {  // this thread is no longer
                                // responsible for this key
            remove_set.insert(key);

            // add all the new threads that this key should be sent to
            for (const ServerThread& thread : threads) {
              addr_keyset_map[thread.gossip_connect_address()].insert(key);
            }
          }

          // decrement represents whether the total global or local rep factor
          // has been reduced; if that's not the case, and I am the "first"
          // thread responsible for this key, then I gossip it to the new
          // threads that are responsible for it
          if (!decrement && orig_threads.begin()->id() == wt.id()) {
            std::unordered_set<ServerThread, ThreadHash> new_threads;

            for (const ServerThread& thread : threads) {
              if (std::find(orig_threads.begin(), orig_threads.end(), thread) ==
                  orig_threads.end()) {
                new_threads.insert(thread);
              }
            }

            for (const ServerThread& thread : new_threads) {
              addr_keyset_map[thread.gossip_connect_address()].insert(key);
            }
          }
        } else {
          log->error(
              "Missing key replication factor in rep factor change routine.");
        }
      } else {
        log->error(
            "Missing key replication factor in rep factor change routine.");

        // just update the replication factor
        for (const auto& global : key_rep.global()) {
          key_replication_map[key].global_replication_[global.tier_id()] =
              global.replication_factor();
        }

        for (const auto& local : key_rep.local()) {
          key_replication_map[key].local_replication_[local.tier_id()] =
              local.replication_factor();
        }
      }
    } else {
      // just update the replication factor
      for (const auto& global : key_rep.global()) {
        key_replication_map[key].global_replication_[global.tier_id()] =
            global.replication_factor();
      }

      for (const auto& local : key_rep.local()) {
        key_replication_map[key].local_replication_[local.tier_id()] =
            local.replication_factor();
      }
    }
  }

  send_gossip(addr_keyset_map, pushers, serializers, stored_key_map);

  // remove keys
  for (const string& key : remove_set) {
    serializers[stored_key_map[key].type_]->remove(key);
    stored_key_map.erase(key);
    local_changeset.erase(key);
  }
}
