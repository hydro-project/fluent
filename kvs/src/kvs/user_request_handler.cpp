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

void user_request_handler(
    unsigned& total_accesses, unsigned& seed, string& serialized,
    std::shared_ptr<spdlog::logger> logger,
    map<TierId, GlobalHashRing>& global_hash_rings,
    map<TierId, LocalHashRing>& local_hash_rings,
    PendingMap<PendingRequest>& pending_request_map,
    map<Key, std::multiset<TimePoint>>& key_access_timestamp,
    map<Key, KeyMetadata>& metadata_map, set<Key>& local_changeset,
    ServerThread& wt, SerializerMap& serializers, SocketCache& pushers) {
  KeyRequest request;
  request.ParseFromString(serialized);

  KeyResponse response;
  string response_id = "";

  if (request.has_request_id()) {
    response_id = request.request_id();
    response.set_response_id(response_id);
  }

  bool succeed;
  string request_type = RequestType_Name(request.type());
  string response_address =
      request.has_response_address() ? request.response_address() : "";

  for (const auto& tuple : request.tuples()) {
    // first check if the thread is responsible for the key
    Key key = tuple.key();
    string payload = tuple.has_payload() ? (std::move(tuple.payload())) : "";

    ServerThreadList threads = kHashRingUtil->get_responsible_threads(
        wt.get_replication_factor_connect_addr(), key, is_metadata(key),
        global_hash_rings, local_hash_rings, metadata_map, pushers,
        kSelfTierIdVector, succeed, seed);

    if (succeed) {
      if (std::find(threads.begin(), threads.end(), wt) == threads.end()) {
        if (is_metadata(key)) {
          // this means that this node is not responsible for this metadata key
          KeyTuple* tp = response.add_tuples();

          tp->set_key(key);
          tp->set_lattice_type(tuple.lattice_type());
          tp->set_error(2);
        } else {
          // if we don't know what threads are responsible, we issue a rep
          // factor request and make the request pending
          kHashRingUtil->issue_replication_factor_request(
              wt.get_replication_factor_connect_addr(), key,
              global_hash_rings[kMemoryTierId], local_hash_rings[kMemoryTierId],
              pushers, seed);

          pending_request_map[key].push_back(
              PendingRequest(request_type, tuple.lattice_type(), payload,
                             response_address, response_id));
        }
      } else {  // if we know the responsible threads, we process the request
        KeyTuple* tp = response.add_tuples();
        tp->set_key(key);
        if (request_type == "GET") {
          if (metadata_map.find(key) == metadata_map.end() || metadata_map[key].type_ == LatticeType::NO) {
            tp->set_error(1);
          } else {
            auto res = process_get(key, serializers[metadata_map[key].type_]);
            tp->set_lattice_type(metadata_map[key].type_);
            tp->set_payload(res.first);
            tp->set_error(res.second);
          }
        } else if (request_type == "PUT") {
          if (tuple.lattice_type() == LatticeType::NO) {
            logger->error("PUT request missing lattice type.");
          } else if (metadata_map.find(key) != metadata_map.end() &&
                     metadata_map[key].type_ != LatticeType::NO  &&
                     metadata_map[key].type_ != tuple.lattice_type()) {
            logger->error(
                "Lattice type mismatch for key {}: query is {} but we expect {}.",
                key, LatticeType_Name(tuple.lattice_type()),
                LatticeType_Name(metadata_map[key].type_));
          } else {
            process_put(key, tuple.lattice_type(), payload,
                        serializers[tuple.lattice_type()], metadata_map);

            local_changeset.insert(key);
            tp->set_error(0);
          }
        } else {
          logger->error("Unknown request type {} in user request handler.",
                        request_type);
        }

        if (tuple.has_address_cache_size() &&
            tuple.address_cache_size() != threads.size()) {
          tp->set_invalidate(true);
        }

        key_access_timestamp[key].insert(std::chrono::system_clock::now());
        total_accesses += 1;
      }
    } else {
      pending_request_map[key].push_back(
          PendingRequest(request_type, tuple.lattice_type(), payload,
                         response_address, response_id));
    }
  }

  if (response.tuples_size() > 0 && request.has_response_address()) {
    string serialized_response;
    response.SerializeToString(&serialized_response);
    kZmqUtil->send_string(serialized_response,
                          &pushers[request.response_address()]);
  }
}
