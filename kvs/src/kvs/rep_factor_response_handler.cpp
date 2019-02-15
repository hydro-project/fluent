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
    std::shared_ptr<spdlog::logger> logger, string& serialized,
    map<unsigned, GlobalHashRing>& global_hash_ring_map,
    map<unsigned, LocalHashRing>& local_hash_ring_map,
    PendingMap<PendingRequest>& pending_request_map,
    PendingMap<PendingGossip>& pending_gossip_map,
    map<Key, std::multiset<TimePoint>>& key_access_timestamp,
    map<Key, KeyInfo>& placement,
    map<Key, std::pair<unsigned, LatticeType>>& key_stat_map,
    set<Key>& local_changeset, ServerThread& wt,
    SerializerMap& serializers,
    SocketCache& pushers) {
  KeyResponse response;
  response.ParseFromString(serialized);

  // we assume tuple 0 because there should only be one tuple responding to a
  // replication factor request
  KeyTuple tuple = response.tuples(0);
  Key key = get_key_from_metadata(tuple.key());

  unsigned error = tuple.error();

  if (error == 0) {
    LWWValue lww_value;
    lww_value.ParseFromString(tuple.payload());
    ReplicationFactor rep_data;
    rep_data.ParseFromString(lww_value.value());

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
    ServerThreadList threads = kHashRingUtil->get_responsible_threads(
        wt.get_replication_factor_connect_addr(), key, is_metadata(key),
        global_hash_ring_map, local_hash_ring_map, placement, pushers,
        kSelfTierIdVector, succeed, seed);

    if (succeed) {
      bool responsible =
          std::find(threads.begin(), threads.end(), wt) != threads.end();

      for (const PendingRequest& request : pending_request_map[key]) {
        auto now = std::chrono::system_clock::now();

        if (!responsible && request.addr_ != "") {
          KeyResponse response;

          if (request.response_id_ != "") {
            response.set_response_id(request.response_id_);
          }

          KeyTuple* tp = response.add_tuples();
          tp->set_key(key);
          tp->set_error(2);

          for (const ServerThread& thread : threads) {
            tp->add_addresses(thread.get_request_pulling_connect_addr());
          }

          string serialized_response;
          response.SerializeToString(&serialized_response);
          kZmqUtil->send_string(serialized_response, &pushers[request.addr_]);
        } else if (responsible && request.addr_ == "") {
          // only put requests should fall into this category
          if (request.type_ == "PUT") {
            if (request.lattice_type_ == LatticeType::NO) {
              logger->error("PUT request missing lattice type.");
            } else if (key_stat_map.find(key) != key_stat_map.end() &&
                       key_stat_map[key].second != request.lattice_type_) {
              logger->error(
                  "Lattice type mismatch: {} from query but {} expected.",
                  LatticeType_Name(request.lattice_type_),
                  key_stat_map[key].second);
            } else {
              process_put(key, request.lattice_type_, request.payload_,
                          serializers[request.lattice_type_], key_stat_map);
              key_access_timestamp[key].insert(now);

              total_access += 1;
              local_changeset.insert(key);
            }
          } else {
            logger->error("Received a GET request with no response address.");
          }
        } else if (responsible && request.addr_ != "") {
          KeyResponse response;

          if (request.response_id_ != "") {
            response.set_response_id(request.response_id_);
          }

          KeyTuple* tp = response.add_tuples();
          tp->set_key(key);

          if (request.type_ == "GET") {
            if (key_stat_map.find(key) == key_stat_map.end()) {
              tp->set_error(1);
            } else {
              auto res =
                  process_get(key, serializers[key_stat_map[key].second]);
              tp->set_lattice_type(key_stat_map[key].second);
              tp->set_payload(res.first);
              tp->set_error(res.second);
            }
          } else {
            if (request.lattice_type_ == LatticeType::NO) {
              logger->error("PUT request missing lattice type.");
            } else if (key_stat_map.find(key) != key_stat_map.end() &&
                       key_stat_map[key].second != request.lattice_type_) {
              logger->error(
                  "Lattice type mismatch: {} from query but {} expected.",
                  LatticeType_Name(request.lattice_type_),
                  key_stat_map[key].second);
            } else {
              process_put(key, request.lattice_type_, request.payload_,
                          serializers[request.lattice_type_], key_stat_map);
              tp->set_error(0);
              local_changeset.insert(key);
            }
          }
          key_access_timestamp[key].insert(now);
          total_access += 1;

          string serialized_response;
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
    ServerThreadList threads = kHashRingUtil->get_responsible_threads(
        wt.get_replication_factor_connect_addr(), key, is_metadata(key),
        global_hash_ring_map, local_hash_ring_map, placement, pushers,
        kSelfTierIdVector, succeed, seed);

    if (succeed) {
      if (std::find(threads.begin(), threads.end(), wt) != threads.end()) {
        for (const PendingGossip& gossip : pending_gossip_map[key]) {
          if (key_stat_map.find(key) != key_stat_map.end() &&
              key_stat_map[key].second != gossip.lattice_type_) {
            logger->error(
                "Lattice type mismatch: {} from query but {} expected.",
                LatticeType_Name(gossip.lattice_type_),
                key_stat_map[key].second);
          } else {
            process_put(key, gossip.lattice_type_, gossip.payload_,
                        serializers[gossip.lattice_type_], key_stat_map);
          }
        }
      } else {
        map<Address, KeyRequest> gossip_map;

        // forward the gossip
        for (const ServerThread& thread : threads) {
          gossip_map[thread.get_gossip_connect_addr()].set_type(
              RequestType::PUT);

          for (const PendingGossip& gossip : pending_gossip_map[key]) {
            prepare_put_tuple(gossip_map[thread.get_gossip_connect_addr()], key,
                              gossip.lattice_type_, gossip.payload_);
          }
        }

        // redirect gossip
        for (const auto& gossip_pair : gossip_map) {
          string serialized;
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
