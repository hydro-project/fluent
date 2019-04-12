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

#include "causal_cache_utils.hpp"

void get_request_handler(
    const string& serialized, set<Key>& key_set, StoreType& unmerged_store,
    InPreparationType& in_preparation, StoreType& causal_cut_store,
    VersionStoreType& version_store,
    map<Key, set<Address>>& single_callback_map,
    map<Address, PendingClientMetadata>& pending_single_metadata,
    map<Address, PendingClientMetadata>& pending_cross_metadata,
    map<Key, set<Key>>& to_fetch_map,
    map<Key, std::unordered_map<VectorClock, set<Key>, VectorClockHash>>&
        cover_map,
    SocketCache& pushers, KvsAsyncClientInterface* client, logger log,
    const CausalCacheThread& cct,
    map<string, set<Address>>& client_id_to_address_map) {
  CausalRequest request;
  request.ParseFromString(serialized);

  bool covered_locally = true;
  set<Key> read_set;
  set<Key> to_cover;

  // check if the keys are covered locally
  if (request.consistency() == ConsistencyType::SINGLE) {
    for (CausalTuple tuple : request.tuples()) {
      Key key = tuple.key();
      read_set.insert(key);
      key_set.insert(key);

      if (unmerged_store.find(key) == unmerged_store.end()) {
        covered_locally = false;
        to_cover.insert(key);
        single_callback_map[key].insert(request.response_address());
        client->get_async(key);
      }
    }
    if (!covered_locally) {
      pending_single_metadata[request.response_address()] =
          PendingClientMetadata(request.id(), read_set, to_cover);
    } else {
      CausalResponse response;
      for (const Key& key : read_set) {
        CausalTuple* tp = response.add_tuples();
        tp->set_key(key);
        tp->set_payload(serialize(*(unmerged_store[key])));
      }

      // send response
      string resp_string;
      response.SerializeToString(&resp_string);
      kZmqUtil->send_string(resp_string, &pushers[request.response_address()]);
    }
  } else if (request.consistency() == ConsistencyType::CROSS) {
    // first, we compute the condensed version of prior causal chains
    map<Key, std::unordered_set<VectorClock, VectorClockHash>> causal_frontier;

    for (const auto& addr_versioned_key_list_pair :
         request.versioned_key_locations()) {
      for (const auto& versioned_key :
           addr_versioned_key_list_pair.second.versioned_keys()) {
        // first, convert protobuf type to VectorClock
        VectorClock vc;
        for (const auto& key_version_pair : versioned_key.vector_clock()) {
          vc.insert(key_version_pair.first, key_version_pair.second);
        }

        populate_causal_frontier(versioned_key.key(), vc, causal_frontier);
      }
    }
    // now we have the causal frontier
    // we can populate prior causal chain
    for (const auto& addr_versioned_key_list_pair :
         request.versioned_key_locations()) {
      for (const auto& versioned_key :
           addr_versioned_key_list_pair.second.versioned_keys()) {
        // first, convert protobuf type to VectorClock
        VectorClock vc;
        for (const auto& key_version_pair : versioned_key.vector_clock()) {
          vc.insert(key_version_pair.first, key_version_pair.second);
        }

        if (causal_frontier[versioned_key.key()].find(vc) !=
            causal_frontier[versioned_key.key()].end()) {
          pending_cross_metadata[request.response_address()]
              .prior_causal_chains_[addr_versioned_key_list_pair.first]
                                   [versioned_key.key()] = vc;
        }
      }
    }
    // set the client id field of PendingClientMetadata
    pending_cross_metadata[request.response_address()].client_id_ =
        request.id();

    // set the future read set field
    for (const Key& key : request.future_read_set()) {
      pending_cross_metadata[request.response_address()]
          .future_read_set_.insert(key);
    }

    for (CausalTuple tuple : request.tuples()) {
      Key key = tuple.key();
      read_set.insert(key);
      key_set.insert(key);

      if (causal_cut_store.find(key) == causal_cut_store.end() &&
          causal_frontier.find(key) == causal_frontier.end()) {
        // check if the key is in in_preparation
        if (in_preparation.find(key) != in_preparation.end()) {
          covered_locally = false;
          to_cover.insert(key);
          in_preparation[key].first.insert(request.response_address());
        } else {
          to_fetch_map[key] = set<Key>();
          // check if key is in one of the sub-key of in_preparation or in
          // unmerged_store
          auto lattice = find_lattice_from_in_preparation(in_preparation, key);
          if (lattice != nullptr) {
            in_preparation[key].second[key] = lattice;
            recursive_dependency_check(key, lattice, in_preparation,
                                       causal_cut_store, unmerged_store,
                                       to_fetch_map, cover_map, client);
            if (to_fetch_map[key].size() == 0) {
              // all dependency met
              merge_into_causal_cut(key, causal_cut_store, in_preparation,
                                    version_store, pending_cross_metadata,
                                    pushers, cct, client_id_to_address_map);
              to_fetch_map.erase(key);
            } else {
              in_preparation[key].first.insert(request.response_address());
              covered_locally = false;
              to_cover.insert(key);
            }
          } else if (unmerged_store.find(key) != unmerged_store.end()) {
            in_preparation[key].second[key] = unmerged_store[key];
            recursive_dependency_check(key, unmerged_store[key], in_preparation,
                                       causal_cut_store, unmerged_store,
                                       to_fetch_map, cover_map, client);
            if (to_fetch_map[key].size() == 0) {
              // all dependency met
              merge_into_causal_cut(key, causal_cut_store, in_preparation,
                                    version_store, pending_cross_metadata,
                                    pushers, cct, client_id_to_address_map);
              to_fetch_map.erase(key);
            } else {
              in_preparation[key].first.insert(request.response_address());
              covered_locally = false;
              to_cover.insert(key);
            }
          } else {
            in_preparation[key].first.insert(request.response_address());
            covered_locally = false;
            to_cover.insert(key);
            client->get_async(key);
          }
        }
      }
    }
    if (!covered_locally) {
      pending_cross_metadata[request.response_address()].read_set_ = read_set;
      pending_cross_metadata[request.response_address()].to_cover_set_ =
          to_cover;
    } else {
      pending_cross_metadata[request.response_address()].read_set_ = read_set;
      // decide local and remote read set
      if (!fire_remote_read_requests(
              pending_cross_metadata[request.response_address()], version_store,
              causal_cut_store, pushers, cct)) {
        // all local
        respond_to_client(pending_cross_metadata, request.response_address(),
                          causal_cut_store, version_store, pushers, cct);
      } else {
        client_id_to_address_map[request.id()].insert(
            request.response_address());
      }
    }
  } else {
    log->error("Found non-causal consistency level.");
  }
}