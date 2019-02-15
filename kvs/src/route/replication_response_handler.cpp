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

#include "route/routing_handlers.hpp"

void replication_response_handler(
    std::shared_ptr<spdlog::logger> logger, string& serialized,
    SocketCache& pushers, RoutingThread& rt,
    vector<GlobalHashRing>& global_hash_rings,
    vector<LocalHashRing>& local_hash_rings,
    map<Key, KeyInfo>& placement,
    PendingMap<std::pair<Address, string>>& pending_key_request_map,
    unsigned& seed) {
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
    auto respond_address = rt.get_replication_factor_connect_addr();
    kHashRingUtil->issue_replication_factor_request(
        respond_address, key, global_hash_rings[1], local_hash_rings[1],
        pushers, seed);
    return;
  } else {
    logger->error("Unexpected error type {} in replication factor response.",
                  error);
    return;
  }

  // process pending key address requests
  if (pending_key_request_map.find(key) != pending_key_request_map.end()) {
    bool succeed;
    unsigned tier_id = 1;
    ServerThreadList threads = {};

    while (threads.size() == 0 && tier_id < kMaxTier) {
      threads = kHashRingUtil->get_responsible_threads(
          rt.get_replication_factor_connect_addr(), key, false,
          global_hash_rings, local_hash_rings, placement, pushers,
          {tier_id}, succeed, seed);

      if (!succeed) {
        logger->error("Missing replication factor for key {}.", key);
        return;
      }

      tier_id++;
    }

    for (const auto& pending_key_req : pending_key_request_map[key]) {
      KeyAddressResponse key_res;
      key_res.set_response_id(pending_key_req.second);
      auto* tp = key_res.add_addresses();
      tp->set_key(key);

      for (const ServerThread& thread : threads) {
        tp->add_ips(thread.get_request_pulling_connect_addr());
      }

      // send the key address response
      key_res.set_error(0);

      string serialized;
      key_res.SerializeToString(&serialized);
      kZmqUtil->send_string(serialized, &pushers[pending_key_req.first]);
    }

    pending_key_request_map.erase(key);
  }
}
