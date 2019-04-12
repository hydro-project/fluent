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

unsigned causal_comparison(
    const std::shared_ptr<CrossCausalLattice<SetLattice<string>>>& lhs,
    const std::shared_ptr<CrossCausalLattice<SetLattice<string>>>& rhs) {
  VectorClock lhs_prev_vc = lhs->reveal().vector_clock;
  VectorClock lhs_vc = lhs->reveal().vector_clock;
  VectorClock rhs_vc = rhs->reveal().vector_clock;
  lhs_vc.merge(rhs_vc);
  if (lhs_prev_vc == lhs_vc) {
    return kCausalGreaterOrEqual;
  } else if (lhs_vc == rhs_vc) {
    return kCausalLess;
  } else {
    return kCausalConcurrent;
  }
}

unsigned vector_clock_comparison(const VectorClock& lhs,
                                 const VectorClock& rhs) {
  VectorClock lhs_prev_vc = lhs;
  VectorClock lhs_vc = lhs;
  VectorClock rhs_vc = rhs;
  lhs_vc.merge(rhs_vc);
  if (lhs_prev_vc == lhs_vc) {
    return kCausalGreaterOrEqual;
  } else if (lhs_vc == rhs_vc) {
    return kCausalLess;
  } else {
    return kCausalConcurrent;
  }
}

std::shared_ptr<CrossCausalLattice<SetLattice<string>>> causal_merge(
    const std::shared_ptr<CrossCausalLattice<SetLattice<string>>>& lhs,
    const std::shared_ptr<CrossCausalLattice<SetLattice<string>>>& rhs) {
  unsigned comp_result = causal_comparison(lhs, rhs);
  if (comp_result == kCausalGreaterOrEqual) {
    return lhs;
  } else if (comp_result == kCausalLess) {
    return rhs;
  } else {
    auto result = std::make_shared<CrossCausalLattice<SetLattice<string>>>();
    result->merge(*lhs);
    result->merge(*rhs);
    return result;
  }
}

std::shared_ptr<CrossCausalLattice<SetLattice<string>>>
find_lattice_from_in_preparation(const InPreparationType& in_preparation,
                                 const Key& key, const VectorClock& vc) {
  for (const auto& pair : in_preparation) {
    for (const auto& inner_pair : pair.second.second) {
      if (inner_pair.first == key &&
          vector_clock_comparison(inner_pair.second->reveal().vector_clock,
                                  vc) == kCausalGreaterOrEqual) {
        return inner_pair.second;
      }
    }
  }
  return nullptr;
}

bool populate_in_preparation(
    const Key& head_key, const Key& dep_key,
    const std::shared_ptr<CrossCausalLattice<SetLattice<string>>>& lattice,
    InPreparationType& in_preparation) {
  if (in_preparation[head_key].second.find(dep_key) ==
      in_preparation[head_key].second.end()) {
    in_preparation[head_key].second[dep_key] = lattice;
    return true;
  } else {
    unsigned comp_result =
        causal_comparison(in_preparation[head_key].second[dep_key], lattice);
    if (comp_result == kCausalLess) {
      in_preparation[head_key].second[dep_key] = lattice;
      return true;
    } else if (comp_result == kCausalConcurrent) {
      in_preparation[head_key].second[dep_key] =
          causal_merge(in_preparation[head_key].second[dep_key], lattice);
      return true;
    } else {
      return false;
    }
  }
}

void recursive_dependency_check(
    const Key& head_key,
    const std::shared_ptr<CrossCausalLattice<SetLattice<string>>>& lattice,
    InPreparationType& in_preparation, const StoreType& causal_cut_store,
    const StoreType& unmerged_store, map<Key, set<Key>>& to_fetch_map,
    map<Key, std::unordered_map<VectorClock, set<Key>, VectorClockHash>>&
        cover_map,
    KvsAsyncClientInterface* client) {
  for (const auto& pair : lattice->reveal().dependency.reveal()) {
    Key dep_key = pair.first;
    // first, check if the dependency is already satisfied in the causal cut
    if (causal_cut_store.find(dep_key) != causal_cut_store.end() &&
        vector_clock_comparison(
            causal_cut_store.at(dep_key)->reveal().vector_clock,
            lattice->reveal().dependency.reveal().at(dep_key)) ==
            kCausalGreaterOrEqual) {
      continue;
    }
    // then, check if the dependency is already satisfied in the in_preparation
    auto target_lattice = find_lattice_from_in_preparation(
        in_preparation, dep_key,
        lattice->reveal().dependency.reveal().at(dep_key));
    if (target_lattice != nullptr) {
      if (populate_in_preparation(head_key, dep_key, target_lattice,
                                  in_preparation)) {
        recursive_dependency_check(head_key, target_lattice, in_preparation,
                                   causal_cut_store, unmerged_store,
                                   to_fetch_map, cover_map, client);
      }
      // in_preparation[head_key].second[dep_key] = target_lattice;
    } else {
      // check if the dependency is satisfied in unmerged_store (unmerged store
      // should always dominate the in_preparation)
      if (unmerged_store.find(dep_key) != unmerged_store.end() &&
          vector_clock_comparison(
              unmerged_store.at(dep_key)->reveal().vector_clock,
              lattice->reveal().dependency.reveal().at(dep_key)) ==
              kCausalGreaterOrEqual) {
        if (populate_in_preparation(head_key, dep_key,
                                    unmerged_store.at(dep_key),
                                    in_preparation)) {
          recursive_dependency_check(head_key, unmerged_store.at(dep_key),
                                     in_preparation, causal_cut_store,
                                     unmerged_store, to_fetch_map, cover_map,
                                     client);
        }
      } else {
        // we issue GET to KVS
        to_fetch_map[head_key].insert(dep_key);
        cover_map[dep_key][lattice->reveal().dependency.reveal().at(dep_key)]
            .insert(head_key);
        client->get_async(dep_key);
      }
    }
  }
}

Address find_address(
    const Key& key, const VectorClock& vc,
    const map<Address, map<Key, VectorClock>>& prior_causal_chains) {
  for (const auto& address_map_pair : prior_causal_chains) {
    for (const auto& key_vc_pair : address_map_pair.second) {
      if (key_vc_pair.first == key &&
          vector_clock_comparison(vc, key_vc_pair.second) == kCausalLess) {
        // find a remote vector clock that dominates the local one, so read from
        // the remote node
        return address_map_pair.first;
      }
    }
  }
  // we are good to read from the local causal cache
  return "";
}

void save_versions(const string& id, const Key& key,
                   VersionStoreType& version_store,
                   const StoreType& causal_cut_store,
                   const set<Key>& future_read_set, set<Key>& observed_keys) {
  if (observed_keys.find(key) == observed_keys.end()) {
    if (future_read_set.find(key) != future_read_set.end()) {
      version_store[id][key] = causal_cut_store.at(key);
    }
    for (const auto& pair :
         causal_cut_store.at(key)->reveal().dependency.reveal()) {
      save_versions(id, pair.first, version_store, causal_cut_store,
                    future_read_set, observed_keys);
    }
  }
}

bool fire_remote_read_requests(PendingClientMetadata& metadata,
                               VersionStoreType& version_store,
                               const StoreType& causal_cut_store,
                               SocketCache& pushers,
                               const CausalCacheThread& cct) {
  // first we determine which key should be read from remote
  bool remote_request = false;

  map<Address, VersionedKeyRequest> addr_request_map;

  for (const Key& key : metadata.read_set_) {
    if (metadata.dne_set_.find(key) != metadata.dne_set_.end()) {
      // the key dne
      metadata.serialized_local_payload_[key] = "";
      continue;
    }
    if (causal_cut_store.find(key) == causal_cut_store.end()) {
      // no key in local causal cache, find a remote and fire request
      remote_request = true;

      Address remote_addr =
          find_address(key, VectorClock(), metadata.prior_causal_chains_);
      if (addr_request_map.find(remote_addr) == addr_request_map.end()) {
        addr_request_map[remote_addr].set_id(metadata.client_id_);
        addr_request_map[remote_addr].set_response_address(
            cct.causal_cache_versioned_key_response_connect_address());
      }
      addr_request_map[remote_addr].add_keys(key);
      metadata.remote_read_set_.insert(key);
    } else {
      Address remote_addr =
          find_address(key, causal_cut_store.at(key)->reveal().vector_clock,
                       metadata.prior_causal_chains_);
      if (remote_addr != "") {
        // we need to read from remote
        remote_request = true;

        if (addr_request_map.find(remote_addr) == addr_request_map.end()) {
          addr_request_map[remote_addr].set_id(metadata.client_id_);
          addr_request_map[remote_addr].set_response_address(
              cct.causal_cache_versioned_key_response_connect_address());
        }
        addr_request_map[remote_addr].add_keys(key);
        metadata.remote_read_set_.insert(key);
      } else {
        // we can read from local
        metadata.serialized_local_payload_[key] =
            serialize(*(causal_cut_store.at(key)));
        // copy pointer to keep the version (only for those in the future read
        // set)
        set<Key> observed_keys;
        save_versions(metadata.client_id_, key, version_store, causal_cut_store,
                      metadata.future_read_set_, observed_keys);
      }
    }
  }

  for (const auto& pair : addr_request_map) {
    // send request
    string req_string;
    pair.second.SerializeToString(&req_string);
    kZmqUtil->send_string(req_string, &pushers[pair.first]);
  }
  return remote_request;
}

void respond_to_client(
    map<Address, PendingClientMetadata>& pending_cross_metadata,
    const Address& addr, const StoreType& causal_cut_store,
    const VersionStoreType& version_store, SocketCache& pushers,
    const CausalCacheThread& cct) {
  CausalResponse response;

  for (const Key& key : pending_cross_metadata[addr].read_set_) {
    CausalTuple* tp = response.add_tuples();
    tp->set_key(key);

    if (pending_cross_metadata[addr].dne_set_.find(key) !=
        pending_cross_metadata[addr].dne_set_.end()) {
      // key dne
      tp->set_error(1);
    } else {
      tp->set_error(0);
      tp->set_payload(serialize(*(causal_cut_store.at(key))));
    }
  }

  response.set_versioned_key_query_addr(
      cct.causal_cache_versioned_key_request_connect_address());

  if (version_store.find(pending_cross_metadata[addr].client_id_) !=
      version_store.end()) {
    for (const auto& pair :
         version_store.at(pending_cross_metadata[addr].client_id_)) {
      VersionedKey* vk = response.add_versioned_keys();
      vk->set_key(pair.first);
      auto ptr = vk->mutable_vector_clock();
      for (const auto& client_version_pair :
           pair.second->reveal().vector_clock.reveal()) {
        (*ptr)[client_version_pair.first] = client_version_pair.second.reveal();
      }
    }
  }

  // send response
  string resp_string;
  response.SerializeToString(&resp_string);
  kZmqUtil->send_string(resp_string, &pushers[addr]);
  // GC
  pending_cross_metadata.erase(addr);
}

void merge_into_causal_cut(
    const Key& key, StoreType& causal_cut_store,
    InPreparationType& in_preparation, VersionStoreType& version_store,
    map<Address, PendingClientMetadata>& pending_cross_metadata,
    SocketCache& pushers, const CausalCacheThread& cct,
    map<string, set<Address>>& client_id_to_address_map) {
  bool key_dne = false;
  // merge from in_preparation to causal_cut_store
  for (const auto& pair : in_preparation[key].second) {
    if (vector_clock_comparison(
            VectorClock(), pair.second->reveal().vector_clock) == kCausalLess) {
      // only merge when the key exists
      if (causal_cut_store.find(pair.first) == causal_cut_store.end()) {
        // key doesn't exist in causal cut store
        causal_cut_store[pair.first] = pair.second;
      } else {
        // we compare two lattices
        unsigned comp_result =
            causal_comparison(causal_cut_store[pair.first], pair.second);
        if (comp_result == kCausalLess) {
          causal_cut_store[pair.first] = pair.second;
        } else if (comp_result == kCausalConcurrent) {
          causal_cut_store[pair.first] =
              causal_merge(causal_cut_store[pair.first], pair.second);
        }
      }
    } else {
      key_dne = true;
    }
  }
  // notify clients
  for (const auto& addr : in_preparation[key].first) {
    if (pending_cross_metadata.find(addr) != pending_cross_metadata.end()) {
      if (key_dne) {
        pending_cross_metadata[addr].dne_set_.insert(key);
      }
      pending_cross_metadata[addr].to_cover_set_.erase(key);
      if (pending_cross_metadata[addr].to_cover_set_.size() == 0) {
        // all keys are covered, safe to read
        // decide local and remote read set
        if (!fire_remote_read_requests(pending_cross_metadata[addr],
                                       version_store, causal_cut_store, pushers,
                                       cct)) {
          // all local
          respond_to_client(pending_cross_metadata, addr, causal_cut_store,
                            version_store, pushers, cct);
        } else {
          client_id_to_address_map[pending_cross_metadata[addr].client_id_]
              .insert(addr);
        }
      }
    }
  }
  // erase the chain in in_preparation
  in_preparation.erase(key);
}

void process_response(
    const Key& key,
    const std::shared_ptr<CrossCausalLattice<SetLattice<string>>>& lattice,
    StoreType& unmerged_store, InPreparationType& in_preparation,
    StoreType& causal_cut_store, VersionStoreType& version_store,
    map<Key, set<Address>>& single_callback_map,
    map<Address, PendingClientMetadata>& pending_single_metadata,
    map<Address, PendingClientMetadata>& pending_cross_metadata,
    map<Key, set<Key>>& to_fetch_map,
    map<Key, std::unordered_map<VectorClock, set<Key>, VectorClockHash>>&
        cover_map,
    SocketCache& pushers, KvsAsyncClientInterface* client, logger log,
    const CausalCacheThread& cct,
    map<string, set<Address>>& client_id_to_address_map) {
  // first, update unmerged store
  if (unmerged_store.find(key) == unmerged_store.end()) {
    // key doesn't exist in unmerged map
    unmerged_store[key] = lattice;
    // check call back addresses for single obj causal consistency
    if (single_callback_map.find(key) != single_callback_map.end()) {
      // notify clients
      for (const auto& addr : single_callback_map[key]) {
        // pending_single_metadata[addr].to_cover_set should have this
        // key, and we remove it
        pending_single_metadata[addr].to_cover_set_.erase(key);
        if (pending_single_metadata[addr].to_cover_set_.size() == 0) {
          CausalResponse response;
          for (const Key& key : pending_single_metadata[addr].read_set_) {
            CausalTuple* tp = response.add_tuples();
            tp->set_key(key);
            tp->set_payload(serialize(*(unmerged_store[key])));
          }

          // send response
          string resp_string;
          response.SerializeToString(&resp_string);
          kZmqUtil->send_string(resp_string, &pushers[addr]);
          // GC
          pending_single_metadata.erase(addr);
        }
      }
      single_callback_map.erase(key);
    }
  } else {
    unsigned comp_result = causal_comparison(unmerged_store.at(key), lattice);
    if (comp_result == kCausalLess) {
      unmerged_store[key] = lattice;
    } else if (comp_result == kCausalConcurrent) {
      unmerged_store[key] = causal_merge(unmerged_store.at(key), lattice);
    }
  }
  // then, inspect the to_fetch_map
  if (to_fetch_map.find(key) != to_fetch_map.end() &&
      to_fetch_map[key].size() == 0) {
    // here, we know that this key is queried by the client directly
    in_preparation[key].second[key] = unmerged_store[key];
    recursive_dependency_check(key, unmerged_store[key], in_preparation,
                               causal_cut_store, unmerged_store, to_fetch_map,
                               cover_map, client);
    if (to_fetch_map[key].size() == 0) {
      // this key has no dependency
      merge_into_causal_cut(key, causal_cut_store, in_preparation,
                            version_store, pending_cross_metadata, pushers, cct,
                            client_id_to_address_map);
      to_fetch_map.erase(key);
    }
  }
  // then, check cover_map to see if this key covers any dependency
  if (cover_map.find(key) != cover_map.end()) {
    std::unordered_map<VectorClock, set<Key>, VectorClockHash> to_remove;
    // track the head keys whose dependency is NOT satisfied
    set<Key> dependency_not_satisfied;
    // track the head keys whose dependency might be satisfied
    set<Key> dependency_may_be_satisfied;
    // loop through the set to see if anything is covered
    for (const auto& pair : cover_map[key]) {
      if (vector_clock_comparison(unmerged_store.at(key)->reveal().vector_clock,
                                  pair.first) == kCausalGreaterOrEqual) {
        for (const auto& head_key : pair.second) {
          // found a dependency that is covered
          in_preparation[head_key].second[key] = unmerged_store[key];
          if (to_fetch_map[head_key].find(key) ==
              to_fetch_map[head_key].end()) {
            log->error("Missing dependency {} in the to_fetch_map of key {}.",
                       key, head_key);
          }
          dependency_may_be_satisfied.insert(head_key);
          to_remove[pair.first].insert(head_key);
        }
      } else {
        for (const auto& head_key : pair.second) {
          dependency_not_satisfied.insert(head_key);
        }
      }
    }
    // only remove from to_fetch_map if the dependency is truly satisfied
    for (const Key& head_key : dependency_may_be_satisfied) {
      if (dependency_not_satisfied.find(head_key) ==
          dependency_not_satisfied.end()) {
        to_fetch_map[head_key].erase(key);
      }
    }

    for (const auto& pair : to_remove) {
      cover_map[key].erase(pair.first);
      for (const auto& head_key : pair.second) {
        recursive_dependency_check(
            head_key, unmerged_store[key], in_preparation, causal_cut_store,
            unmerged_store, to_fetch_map, cover_map, client);
        if (to_fetch_map[head_key].size() == 0) {
          // all dependency is met
          merge_into_causal_cut(head_key, causal_cut_store, in_preparation,
                                version_store, pending_cross_metadata, pushers,
                                cct, client_id_to_address_map);
          to_fetch_map.erase(head_key);
        }
      }
    }
    if (cover_map[key].size() == 0) {
      cover_map.erase(key);
    } else {
      // not fully covered, so we re-issue the read request
      client->get_async(key);
    }
  }
}

void populate_causal_frontier(
    const Key& key, const VectorClock& vc,
    map<Key, std::unordered_set<VectorClock, VectorClockHash>>&
        causal_frontier) {
  std::unordered_set<VectorClock, VectorClockHash> to_remove;

  for (const VectorClock& frontier_vc : causal_frontier[key]) {
    if (vector_clock_comparison(frontier_vc, vc) == kCausalLess) {
      to_remove.insert(frontier_vc);
    } else if (vector_clock_comparison(frontier_vc, vc) ==
               kCausalGreaterOrEqual) {
      return;
    }
  }

  for (const VectorClock& to_remove_vc : to_remove) {
    causal_frontier[key].erase(to_remove_vc);
  }

  causal_frontier[key].insert(vc);
}