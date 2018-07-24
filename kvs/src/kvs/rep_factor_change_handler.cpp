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

void rep_factor_change_handler(
    Address ip, unsigned thread_id, unsigned& seed,
    std::shared_ptr<spdlog::logger> logger, std::string& serialized,
    std::unordered_map<unsigned, GlobalHashRing>& global_hash_ring_map,
    std::unordered_map<unsigned, LocalHashRing>& local_hash_ring_map,
    std::unordered_map<Key, KeyInfo>& placement,
    std::unordered_map<Key, unsigned>& key_size_map,
    std::unordered_set<Key>& local_changeset, ServerThread& wt,
    Serializer* serializer, SocketCache& pushers) {
  logger->info("Received a replication factor change.");
  if (thread_id == 0) {
    // tell all worker threads about the replication factor change
    for (unsigned tid = 1; tid < kThreadNum; tid++) {
      kZmqUtil->send_string(
          serialized,
          &pushers[ServerThread(ip, tid)
                       .get_replication_factor_change_connect_addr()]);
    }
  }

  ReplicationFactorUpdate rep_change;
  rep_change.ParseFromString(serialized);

  AddressKeysetMap addr_keyset_map;
  std::unordered_set<Key> remove_set;

  // for every key, update the replication factor and check if the node is still
  // responsible for the key
  bool succeed;

  for (const ReplicationFactor& key_rep : rep_change.key_reps()) {
    Key key = key_rep.key();

    // if this thread was responsible for the key before the change
    if (key_size_map.find(key) != key_size_map.end()) {
      ServerThreadSet orig_threads = kHashRingUtil->get_responsible_threads(
          wt.get_replication_factor_connect_addr(), key, is_metadata(key),
          global_hash_ring_map, local_hash_ring_map, placement, pushers,
          kAllTierIds, succeed, seed);

      if (succeed) {
        // update the replication factor
        bool decrement = false;

        for (const auto& global : key_rep.global()) {
          if (global.replication_factor() <
              placement[key].global_replication_map_[global.tier_id()]) {
            decrement = true;
          }

          placement[key].global_replication_map_[global.tier_id()] =
              global.replication_factor();
        }

        for (const auto& local : key_rep.local()) {
          if (local.replication_factor() <
              placement[key].local_replication_map_[local.tier_id()]) {
            decrement = true;
          }

          placement[key].local_replication_map_[local.tier_id()] =
              local.replication_factor();
        }

        ServerThreadSet threads = kHashRingUtil->get_responsible_threads(
            wt.get_replication_factor_connect_addr(), key, is_metadata(key),
            global_hash_ring_map, local_hash_ring_map, placement, pushers,
            kAllTierIds, succeed, seed);

        if (succeed) {
          if (threads.find(wt) == threads.end()) {  // this thread is no longer
                                                    // responsible for this key
            remove_set.insert(key);

            // add all the new threads that this key should be sent to
            for (const ServerThread& thread : threads) {
              addr_keyset_map[thread.get_gossip_connect_addr()].insert(key);
            }
          }

          // decrement represents whether the total global or local rep factor
          // has been reduced; if that's not the case, and I am the "first"
          // thread responsible for this key, then I gossip it to the new
          // threads that are responsible for it
          if (!decrement && orig_threads.begin()->get_id() == wt.get_id()) {
            std::unordered_set<ServerThread, ThreadHash> new_threads;

            for (const ServerThread& thread : threads) {
              if (orig_threads.find(thread) == orig_threads.end()) {
                new_threads.insert(thread);
              }
            }

            for (const ServerThread& thread : new_threads) {
              addr_keyset_map[thread.get_gossip_connect_addr()].insert(key);
            }
          }
        } else {
          logger->error(
              "Missing key replication factor in rep factor change routine.");
        }
      } else {
        logger->error(
            "Missing key replication factor in rep factor change routine.");

        // just update the replication factor
        for (const auto& global : key_rep.global()) {
          placement[key].global_replication_map_[global.tier_id()] =
              global.replication_factor();
        }

        for (const auto& local : key_rep.local()) {
          placement[key].local_replication_map_[local.tier_id()] =
              local.replication_factor();
        }
      }
    } else {
      // just update the replication factor
      for (const auto& global : key_rep.global()) {
        placement[key].global_replication_map_[global.tier_id()] =
            global.replication_factor();
      }

      for (const auto& local : key_rep.local()) {
        placement[key].local_replication_map_[local.tier_id()] =
            local.replication_factor();
      }
    }
  }

  send_gossip(addr_keyset_map, pushers, serializer);

  // remove keys
  for (const std::string& key : remove_set) {
    key_size_map.erase(key);
    serializer->remove(key);
    local_changeset.erase(key);
  }
}
