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

void rep_factor_response_handler(
    unsigned& seed, unsigned& total_access,
    std::shared_ptr<spdlog::logger> logger, std::string& serialized,
    std::chrono::system_clock::time_point& start_time,
    std::unordered_map<unsigned, GlobalHashRing>& global_hash_ring_map,
    std::unordered_map<unsigned, LocalHashRing>& local_hash_ring_map,
    PendingMap<PendingRequest>& pending_request_map,
    PendingMap<PendingGossip>& pending_gossip_map,
    std::unordered_map<
        Key, std::multiset<std::chrono::time_point<std::chrono::system_clock>>>&
        key_access_timestamp,
    std::unordered_map<Key, KeyInfo>& placement,
    std::unordered_map<Key, unsigned>& key_size_map,
    std::unordered_set<Key>& local_changeset, ServerThread& wt,
    Serializer* serializer, SocketCache& pushers) {
  KeyResponse response;
  response.ParseFromString(serialized);

  // we assume tuple 0 because there should only be one tuple responding to a
  // replication factor request
  KeyTuple tuple = response.tuples(0);
  Key key = get_key_from_metadata(tuple.key());

  unsigned error = tuple.error();

  if (error == 0) {
    ReplicationFactor rep_data;
    rep_data.ParseFromString(tuple.value());

    for (const auto& global : rep_data.global()) {
      placement[key].global_replication_map_[global.tier_id()] =
          global.replication_factor();
    }

    for (const auto& local : rep_data.local()) {
      placement[key].local_replication_map_[local.tier_id()] =
          local.replication_factor();
    }
  } else if (error == 1) {
    // error 1 means that the receiving thread was responsible for the metadata
    // but didn't have any values stored -- we use the default rep factor
    init_replication(placement, key);
  } else if (error == 2) {
    // error 2 means that the node that received the rep factor request was not
    // responsible for that metadata
    auto respond_address = wt.get_replication_factor_connect_addr();
    kHashRingUtil->issue_replication_factor_request(
        respond_address, key, global_hash_ring_map[1], local_hash_ring_map[1],
        pushers, seed);
    return;
  } else {
    logger->error("Unexpected error type {} in replication factor response.",
                  error);
    return;
  }

  bool succeed;

  if (pending_request_map.find(key) != pending_request_map.end()) {
    ServerThreadSet threads = kHashRingUtil->get_responsible_threads(
        wt.get_replication_factor_connect_addr(), key, is_metadata(key),
        global_hash_ring_map, local_hash_ring_map, placement, pushers,
        kSelfTierIdVector, succeed, seed);

    if (succeed) {
      bool responsible = threads.find(wt) != threads.end();

      for (const PendingRequest& request : pending_request_map[key]) {
        auto now = std::chrono::system_clock::now();

        if (!responsible && request.addr_ != "") {
          KeyResponse response;

          if (request.respond_id_ != "") {
            response.set_response_id(request.respond_id_);
          }

          KeyTuple* tp = response.add_tuples();
          tp->set_key(key);
          tp->set_error(2);

          for (const ServerThread& thread : threads) {
            tp->add_addresses(thread.get_request_pulling_connect_addr());
          }

          std::string serialized_response;
          response.SerializeToString(&serialized_response);
          kZmqUtil->send_string(serialized_response, &pushers[request.addr_]);
        } else if (responsible && request.addr_ == "") {
          // only put requests should fall into this category
          if (request.type_ == "P") {
            auto time_diff =
                std::chrono::duration_cast<std::chrono::milliseconds>(
                    now - start_time)
                    .count();
            auto ts = generate_timestamp(time_diff, wt.get_tid());

            process_put(key, ts, request.value_, serializer, key_size_map);
            key_access_timestamp[key].insert(now);

            total_access += 1;
            local_changeset.insert(key);
          } else {
            logger->error("Received a GET request with no response address.");
          }
        } else if (responsible && request.addr_ != "") {
          KeyResponse response;

          if (request.respond_id_ != "") {
            response.set_response_id(request.respond_id_);
          }

          KeyTuple* tp = response.add_tuples();
          tp->set_key(key);

          if (request.type_ == "G") {
            auto res = process_get(key, serializer);
            tp->set_value(res.first.reveal().value);
            tp->set_error(res.second);

            key_access_timestamp[key].insert(std::chrono::system_clock::now());
            total_access += 1;
          } else {
            auto time_diff =
                std::chrono::duration_cast<std::chrono::milliseconds>(
                    now - start_time)
                    .count();
            auto ts = generate_timestamp(time_diff, wt.get_tid());

            process_put(key, ts, request.value_, serializer, key_size_map);
            tp->set_error(0);

            key_access_timestamp[key].insert(now);
            total_access += 1;
            local_changeset.insert(key);
          }

          std::string serialized_response;
          response.SerializeToString(&serialized_response);
          kZmqUtil->send_string(serialized_response, &pushers[request.addr_]);
        }
      }
    } else {
      logger->error(
          "Missing key replication factor in process pending request routine.");
    }

    pending_request_map.erase(key);
  }

  if (pending_gossip_map.find(key) != pending_gossip_map.end()) {
    ServerThreadSet threads = kHashRingUtil->get_responsible_threads(
        wt.get_replication_factor_connect_addr(), key, is_metadata(key),
        global_hash_ring_map, local_hash_ring_map, placement, pushers,
        kSelfTierIdVector, succeed, seed);

    if (succeed) {
      if (threads.find(wt) != threads.end()) {
        for (const PendingGossip& gossip : pending_gossip_map[key]) {
          process_put(key, gossip.ts_, gossip.value_, serializer, key_size_map);
        }
      } else {
        std::unordered_map<Address, KeyRequest> gossip_map;

        // forward the gossip
        for (const ServerThread& thread : threads) {
          gossip_map[thread.get_gossip_connect_addr()].set_type(
              get_request_type("PUT"));

          for (const PendingGossip& gossip : pending_gossip_map[key]) {
            prepare_put_tuple(gossip_map[thread.get_gossip_connect_addr()], key,
                              gossip.value_, gossip.ts_);
          }
        }

        // redirect gossip
        for (const auto& gossip_pair : gossip_map) {
          std::string serialized;
          gossip_pair.second.SerializeToString(&serialized);
          kZmqUtil->send_string(serialized, &pushers[gossip_pair.first]);
        }
      }
    } else {
      logger->error(
          "Missing key replication factor in process pending gossip routine.");
    }

    pending_gossip_map.erase(key);
  }
}
