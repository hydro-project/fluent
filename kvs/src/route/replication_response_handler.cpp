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
    logger log, string& serialized, SocketCache& pushers, RoutingThread& rt,
    map<TierId, GlobalHashRing>& global_hash_rings,
    map<TierId, LocalHashRing>& local_hash_rings,
    map<Key, KeyReplication>& key_replication_map,
    map<Key, vector<pair<Address, string>>>& pending_requests, unsigned& seed) {
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
      key_replication_map[key].global_replication_[global.tier_id()] =
          global.replication_factor();
    }

    for (const auto& local : rep_data.local()) {
      key_replication_map[key].local_replication_[local.tier_id()] =
          local.replication_factor();
    }
  } else if (error == 1) {
    // error 1 means that the receiving thread was responsible for the metadata
    // but didn't have any values stored -- we use the default rep factor
    init_replication(key_replication_map, key);
  } else if (error == 2) {
    // error 2 means that the node that received the rep factor request was not
    // responsible for that metadata
    auto respond_address = rt.replication_response_connect_address();
    kHashRingUtil->issue_replication_factor_request(
        respond_address, key, global_hash_rings[kMemoryTierId],
        local_hash_rings[kMemoryTierId], pushers, seed);
    return;
  } else {
    log->error("Unexpected error type {} in replication factor response.",
               error);
    return;
  }

  // process pending key address requests
  if (pending_requests.find(key) != pending_requests.end()) {
    bool succeed;
    unsigned tier_id = 0;
    ServerThreadList threads = {};

    while (threads.size() == 0 && tier_id < kMaxTier) {
      threads = kHashRingUtil->get_responsible_threads(
          rt.replication_response_connect_address(), key, false,
          global_hash_rings, local_hash_rings, key_replication_map, pushers,
          {tier_id}, succeed, seed);

      if (!succeed) {
        log->error("Missing replication factor for key {}.", key);
        return;
      }

      tier_id++;
    }

    for (const auto& pending_key_req : pending_requests[key]) {
      KeyAddressResponse key_res;
      key_res.set_response_id(pending_key_req.second);
      auto* tp = key_res.add_addresses();
      tp->set_key(key);

      for (const ServerThread& thread : threads) {
        tp->add_ips(thread.key_request_connect_address());
      }

      // send the key address response
      key_res.set_error(0);

      string serialized;
      key_res.SerializeToString(&serialized);
      kZmqUtil->send_string(serialized, &pushers[pending_key_req.first]);
    }

    pending_requests.erase(key);
  }
}
