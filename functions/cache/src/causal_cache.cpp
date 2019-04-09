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

#include "functions.pb.h"
#include "kvs_async_client.hpp"
#include "yaml-cpp/yaml.h"

ZmqUtil zmq_util;
ZmqUtilInterface* kZmqUtil = &zmq_util;

unsigned kCausalCacheReportThreshold = 5;
unsigned kMigrateThreshold = 10;

unsigned kCausalGreaterOrEqual = 0;
unsigned kCausalLess = 1;
unsigned kCausalConcurrent = 2;

using StoreType =
    map<Key, std::shared_ptr<CrossCausalLattice<SetLattice<string>>>>;
using InPreparationType = map<
    Key,
    pair<set<Address>,
         map<Key, std::shared_ptr<CrossCausalLattice<SetLattice<string>>>>>>;
using VersionStoreType =
    map<string,
        map<Key, std::shared_ptr<CrossCausalLattice<SetLattice<string>>>>>;

struct PendingClientMetadata {
  PendingClientMetadata() = default;

  PendingClientMetadata(string client_id, set<Key> read_set,
                        set<Key> to_cover_set) :
      client_id_(std::move(client_id)),
      read_set_(std::move(read_set)),
      to_cover_set_(std::move(to_cover_set)) {}

  string client_id_;
  set<Key> read_set_;
  set<Key> to_cover_set_;
  map<Address, map<Key, VectorClock>> prior_causal_chains_;
  set<Key> future_read_set_;
  set<Key> remote_read_set_;
  map<Key, string> serialized_local_payload_;
  map<Key, string> serialized_remote_payload_;
};

struct VectorClockHash {
  std::size_t operator()(const VectorClock& vc) const {
    std::size_t result = std::hash<int>()(-1);
    for (const auto& pair : vc.reveal()) {
      result = result ^ std::hash<string>()(pair.first) ^
               std::hash<unsigned>()(pair.second.reveal());
    }
    return result;
  }
};

// given two cross causal lattices (of the same key), compare which one is
// bigger
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

// given two vector clocks (of the same key), compare which one is bigger
unsigned vc_comparison(const VectorClock& lhs, const VectorClock& rhs) {
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

// may be more efficient to find the smallest lattice that
// satisfies the condition...
// maybe prioritize head key search?
std::shared_ptr<CrossCausalLattice<SetLattice<string>>>
find_lattice_from_in_preparation(const InPreparationType& in_preparation,
                                 const Key& key, const VectorClock& vc = VectorClock()) {
  for (const auto& pair : in_preparation) {
    for (const auto& inner_pair : pair.second.second) {
      if (inner_pair.first == key &&
          vc_comparison(inner_pair.second->reveal().vector_clock, vc) ==
              kCausalGreaterOrEqual) {
        return inner_pair.second;
      }
    }
  }
  return nullptr;
}

// return true if this lattice is not dominated by what's already in the in_preparation map
// used to eliminate potential infinite loop
bool populate_in_preparation(
    const Key& head_key, const Key& dep_key,
    const std::shared_ptr<CrossCausalLattice<SetLattice<string>>>& lattice,
    InPreparationType& in_preparation) {
  if (in_preparation[head_key].second.find(dep_key) ==
      in_preparation[head_key].second.end()) {
    in_preparation[head_key].second[dep_key] = lattice;
    return true;
  } else {
    bool comp_result =
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
    map<Key, std::unordered_map<VectorClock, set<Key>, VectorClockHash>>& cover_map,
    KvsAsyncClient& client) {
  for (const auto& dep_key : lattice->reveal().dependency.key_set().reveal()) {
    // first, check if the dependency is already satisfied in the causal cut
    if (causal_cut_store.find(dep_key) != causal_cut_store.end() &&
        vc_comparison(causal_cut_store.at(dep_key)->reveal().vector_clock,
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
          vc_comparison(unmerged_store.at(dep_key)->reveal().vector_clock,
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
        client.get_async(dep_key);
      }
    }
  }
}

Address find_address(const Key& key, const VectorClock& vc, const map<Address, map<Key, VectorClock>>& prior_causal_chains) {
  for (const auto& address_map_pair : prior_causal_chains) {
    for (const auto& key_vc_pair : address_map_pair.second) {
      if (key_vc_pair.first == key && vc_comparison(vc, key_vc_pair.second) == kCausalLess) {
        // find a remote vector clock that dominates the local one, so read from the remote node
        return address_map_pair.first;
      }
    }
  }
  // we are good to read from the local causal cache
  return "";
}

// observed_key is initially passed in as an empty set
// to prevent infinite loop
void save_versions(const string& id, const Key& key,
                   VersionStoreType& version_store,
                   const StoreType& causal_cut_store,
                   const set<Key>& future_read_set,
                   set<Key>& observed_keys) {
  if (observed_keys.find(key) == observed_keys.end()) {
    if (future_read_set.find(key) != future_read_set.end()) {
      version_store[id][key] = causal_cut_store.at(key);
    }
    for (const auto& dep_key :
         causal_cut_store.at(key)->reveal().dependency.key_set().reveal()) {
      save_versions(id, dep_key, version_store, causal_cut_store, future_read_set, observed_keys);
    }
  }
}

bool fire_remote_read_requests(PendingClientMetadata& metadata, VersionStoreType& version_store, const StoreType& causal_cut_store, SocketCache& pushers, const CausalCacheThread& cct) {
  // first we determine which key should be read from remote
  bool remote_request = false;

  map<Address, VersionedKeyRequest> addr_request_map;

  for (const Key& key : metadata.read_set_) {
    if (causal_cut_store.find(key) == causal_cut_store.end()) {
      // no key in local causal cache, find a remote and fire request
      remote_request = true;

      Address remote_addr = find_address(key, VectorClock(), metadata.prior_causal_chains_);
      if (addr_request_map.find(remote_addr) == addr_request_map.end()) {
        addr_request_map[remote_addr].set_id(metadata.client_id_);
        addr_request_map[remote_addr].set_response_address(cct.causal_cache_versioned_key_response_connect_address());
      }
      addr_request_map[remote_addr].add_keys(key);
      metadata.remote_read_set_.insert(key);
    } else {
      Address remote_addr = find_address(key, causal_cut_store.at(key)->reveal().vector_clock, metadata.prior_causal_chains_);
      if (remote_addr != "") {
        // we need to read from remote
        remote_request = true;

        if (addr_request_map.find(remote_addr) == addr_request_map.end()) {
          addr_request_map[remote_addr].set_id(metadata.client_id_);
          addr_request_map[remote_addr].set_response_address(cct.causal_cache_versioned_key_response_connect_address());
        }
        addr_request_map[remote_addr].add_keys(key);
        metadata.remote_read_set_.insert(key);
      } else {
        // we can read from local
        metadata.serialized_local_payload_[key] = serialize(*(causal_cut_store.at(key)));
        // copy pointer to keep the version (only for those in the future read set)
        set<Key> observed_keys;
        save_versions(metadata.client_id_, key, version_store, causal_cut_store, metadata.future_read_set_, observed_keys);
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

void merge_into_causal_cut(
    const Key& key, StoreType& causal_cut_store,
    InPreparationType& in_preparation, VersionStoreType& version_store,
    map<Address, PendingClientMetadata>& pending_cross_request_read_set,
    SocketCache& pushers,
    const CausalCacheThread& cct,
    map<string, set<Address>>& get_client_id_to_address_map) {
  // merge from in_preparation to causal_cut_store
  for (const auto& pair : in_preparation[key].second) {
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
  }
  // notify clients
  for (const auto& addr : in_preparation[key].first) {
    if (pending_cross_request_read_set.find(addr) !=
        pending_cross_request_read_set.end()) {
      pending_cross_request_read_set[addr].to_cover_set_.erase(key);
      if (pending_cross_request_read_set[addr].to_cover_set_.size() == 0) {
        // all keys are covered, safe to read
        // decide local and remote read set
        if (!fire_remote_read_requests(pending_cross_request_read_set[addr], version_store, causal_cut_store, pushers, cct)) {
          // all local
          CausalResponse response;

          for (const Key& key : pending_cross_request_read_set[addr].read_set_) {
            CausalTuple* tp = response.add_tuples();
            tp->set_key(key);
            tp->set_payload(serialize(*(causal_cut_store[key])));
          }

          response.set_versioned_key_query_addr(cct.causal_cache_versioned_key_request_connect_address());

          for (const auto& pair : version_store[pending_cross_request_read_set[addr].client_id_]) {
            VersionedKey* vk = response.add_versioned_keys();
            vk->set_key(pair.first);
            auto ptr = vk->mutable_vector_clock();
            for (const auto& client_version_pair : pair.second->reveal().vector_clock.reveal()) {
              (*ptr)[client_version_pair.first] = client_version_pair.second.reveal();
            }
          }

          // send response
          string resp_string;
          response.SerializeToString(&resp_string);
          kZmqUtil->send_string(resp_string,
                                &pushers[addr]);
          // GC
          pending_cross_request_read_set.erase(addr);
        } else {
          get_client_id_to_address_map[pending_cross_request_read_set[addr].client_id_].insert(addr);
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
    map<Address, PendingClientMetadata>& pending_single_request_read_set,
    map<Address, PendingClientMetadata>& pending_cross_request_read_set,
    map<Key, set<Key>>& to_fetch_map,
    map<Key, std::unordered_map<VectorClock, set<Key>, VectorClockHash>>& cover_map,
    SocketCache& pushers, KvsAsyncClient& client, logger log,
    const CausalCacheThread cct,
    map<string, set<Address>>& get_client_id_to_address_map) {
  // first, update unmerged store
  if (unmerged_store.find(key) == unmerged_store.end()) {
    // key doesn't exist in unmerged map
    unmerged_store[key] = lattice;
    // check call back addresses for single obj causal consistency
    if (single_callback_map.find(key) != single_callback_map.end()) {
      // notify clients
      for (const auto& addr : single_callback_map[key]) {
        // pending_single_request_read_set[addr].to_cover_set should have this
        // key, and we remove it
        pending_single_request_read_set[addr].to_cover_set_.erase(key);
        if (pending_single_request_read_set[addr].to_cover_set_.size() == 0) {
          CausalResponse response;
          for (const Key& key :
               pending_single_request_read_set[addr].read_set_) {
            CausalTuple* tp = response.add_tuples();
            tp->set_key(key);
            tp->set_payload(serialize(*(unmerged_store[key])));
          }

          // send response
          string resp_string;
          response.SerializeToString(&resp_string);
          kZmqUtil->send_string(resp_string, &pushers[addr]);
          // GC
          pending_single_request_read_set.erase(addr);
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
                            version_store, pending_cross_request_read_set,
                            pushers, cct, get_client_id_to_address_map);
      to_fetch_map.erase(key);
    }
  }
  // then, check cover_map to see if this key covers any dependency
  if (cover_map.find(key) != cover_map.end()) {
    std::unordered_map<VectorClock, set<Key>, VectorClockHash> to_remove;
    // loop through the set to see if anything is covered
    for (const auto& pair : cover_map[key]) {
      if (vc_comparison(unmerged_store.at(key)->reveal().vector_clock,
                        pair.first) == kCausalGreaterOrEqual) {
        for (const auto& head_key : pair.second) {
          // found a dependency that is covered
          in_preparation[head_key].second[key] = unmerged_store[key];
          if (to_fetch_map[head_key].find(key) ==
              to_fetch_map[head_key].end()) {
            log->error("Missing dependency {} in the to_fetch_map of key {}.",
                       key, head_key);
          }
          to_fetch_map[head_key].erase(key);
          to_remove[pair.first].insert(head_key);
        }
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
                                version_store, pending_cross_request_read_set,
                                pushers, cct, get_client_id_to_address_map);
          to_fetch_map.erase(head_key);
        }
      }
    }
    if (cover_map[key].size() == 0) {
      cover_map.erase(key);
    } else {
      // not fully covered, so we re-issue the read request
      client.get_async(key);
    }
  }
}

void populate_causal_frontier(const Key& key, const VectorClock& vc, map<Key, std::unordered_set<VectorClock, VectorClockHash>>& causal_frontier) {
  bool dominated = false;
  std::unordered_set<VectorClock, VectorClockHash> to_remove;

  for (const VectorClock& frontier_vc : causal_frontier[key]) {
    if (vc_comparison(frontier_vc, vc) == kCausalLess) {
      to_remove.insert(frontier_vc);
    } else if (vc_comparison(frontier_vc, vc) == kCausalGreaterOrEqual) {
      return;
    }
  }

  for (const VectorClock& to_remove_vc : to_remove) {
    causal_frontier[key].erase(to_remove_vc);
  }

  causal_frontier[key].insert(vc);
}

void run(KvsAsyncClient& client, Address ip, unsigned thread_id) {
  string log_file = "causal_cache_log_" + std::to_string(thread_id) + ".txt";
  string log_name = "causal_cache_log_" + std::to_string(thread_id);
  auto log = spdlog::basic_logger_mt(log_name, log_file, true);
  log->flush_on(spdlog::level::info);

  zmq::context_t* context = client.get_context();

  SocketCache pushers(context, ZMQ_PUSH);

  // keep track of keys that this causal cache is responsible for
  set<Key> key_set;

  StoreType unmerged_store;
  InPreparationType in_preparation;
  StoreType causal_cut_store;
  VersionStoreType version_store;

  map<Key, set<Key>> to_fetch_map;
  map<Key, std::unordered_map<VectorClock, set<Key>, VectorClockHash>> cover_map;

  map<Key, set<Address>> single_callback_map;

  map<Address, PendingClientMetadata> pending_single_request_read_set;
  map<Address, PendingClientMetadata> pending_cross_request_read_set;

  // mapping from client id to a set of response address of GET request
  map<string, set<Address>> get_client_id_to_address_map;

  // mapping from request id to response address of PUT request
  map<string, Address> put_request_id_to_address_map;

  CausalCacheThread cct = CausalCacheThread(ip, thread_id);

  // TODO: can we find a way to make the thread classes uniform across
  // languages? or unify the python and cpp implementations; actually, mostly
  // just the user thread stuff, I think.
  zmq::socket_t get_puller(*context, ZMQ_PULL);
  get_puller.bind(cct.causal_cache_get_bind_address());

  zmq::socket_t put_puller(*context, ZMQ_PULL);
  put_puller.bind(cct.causal_cache_put_bind_address());

  zmq::socket_t update_puller(*context, ZMQ_PULL);
  update_puller.bind(cct.causal_cache_update_bind_address());

  zmq::socket_t version_gc_puller(*context, ZMQ_PULL);
  version_gc_puller.bind(cct.causal_cache_version_gc_bind_address());

  zmq::socket_t versioned_key_request_puller(*context, ZMQ_PULL);
  versioned_key_request_puller.bind(
      cct.causal_cache_versioned_key_request_bind_address());

  zmq::socket_t versioned_key_response_puller(*context, ZMQ_PULL);
  versioned_key_response_puller.bind(
      cct.causal_cache_versioned_key_response_bind_address());

  vector<zmq::pollitem_t> pollitems = {
      {static_cast<void*>(get_puller), 0, ZMQ_POLLIN, 0},
      {static_cast<void*>(put_puller), 0, ZMQ_POLLIN, 0},
      {static_cast<void*>(update_puller), 0, ZMQ_POLLIN, 0},
      {static_cast<void*>(version_gc_puller), 0, ZMQ_POLLIN, 0},
      {static_cast<void*>(versioned_key_request_puller), 0, ZMQ_POLLIN, 0},
      {static_cast<void*>(versioned_key_response_puller), 0, ZMQ_POLLIN, 0},
  };

  auto report_start = std::chrono::system_clock::now();
  auto report_end = std::chrono::system_clock::now();

  auto migrate_start = std::chrono::system_clock::now();
  auto migrate_end = std::chrono::system_clock::now();

  while (true) {
    kZmqUtil->poll(0, &pollitems);

    // handle a GET request
    if (pollitems[0].revents & ZMQ_POLLIN) {
      string serialized = kZmqUtil->recv_string(&get_puller);
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
            client.get_async(key);
          }
        }
        if (!covered_locally) {
          pending_single_request_read_set[request.response_address()] =
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
          kZmqUtil->send_string(resp_string,
                                &pushers[request.response_address()]);
        }
      } else if (request.consistency() == ConsistencyType::CROSS) {
        // first, we compute the condensed version of prior causal chains
        map<Key, std::unordered_set<VectorClock, VectorClockHash>> causal_frontier;

        for (const auto& addr_versioned_key_list_pair : request.address_to_versioned_key_list_map()) {
          for (const auto& versioned_key : addr_versioned_key_list_pair.second.versioned_keys()) {
            // first, convert protobuf type to VectorClock
            VectorClock vc;
            for (const auto& key_version_pair: versioned_key.vector_clock()) {
              vc.insert(key_version_pair.first, key_version_pair.second);
            }
            
            populate_causal_frontier(versioned_key.key(), vc, causal_frontier);
          }
        }
        // now we have the causal frontier
        // we can populate prior causal chain
        for (const auto& addr_versioned_key_list_pair : request.address_to_versioned_key_list_map()) {
          for (const auto& versioned_key : addr_versioned_key_list_pair.second.versioned_keys()) {
            // first, convert protobuf type to VectorClock
            VectorClock vc;
            for (const auto& key_version_pair: versioned_key.vector_clock()) {
              vc.insert(key_version_pair.first, key_version_pair.second);
            }

            if (causal_frontier[versioned_key.key()].find(vc) != causal_frontier[versioned_key.key()].end()) {
              pending_cross_request_read_set[request.response_address()].prior_causal_chains_[addr_versioned_key_list_pair.first][versioned_key.key()] = vc;
            }
          }
        }
        // set the client id field of PendingClientMetadata
        pending_cross_request_read_set[request.response_address()].client_id_ = request.id();

        // set the future read set field
        for (const Key& key : request.future_read_set()) {
          pending_cross_request_read_set[request.response_address()].future_read_set_.insert(key);
        }

        for (CausalTuple tuple : request.tuples()) {
          Key key = tuple.key();
          read_set.insert(key);
          key_set.insert(key);

          if (causal_cut_store.find(key) == causal_cut_store.end() && causal_frontier.find(key) == causal_frontier.end()) {
            // check if the key is in in_preparation
            if (in_preparation.find(key) != in_preparation.end()) {
              covered_locally = false;
              to_cover.insert(key);
              in_preparation[key].first.insert(request.response_address());
            } else {
              to_fetch_map[key] = set<Key>();
              // check if key is in one of the sub-key of in_preparation or in
              // unmerged_store
              auto lattice =
                  find_lattice_from_in_preparation(in_preparation, key);
              if (lattice != nullptr) {
                in_preparation[key].second[key] = lattice;
                recursive_dependency_check(key, lattice, in_preparation,
                                           causal_cut_store, unmerged_store,
                                           to_fetch_map, cover_map, client);
                if (to_fetch_map[key].size() == 0) {
                  // all dependency met
                  merge_into_causal_cut(
                      key, causal_cut_store, in_preparation, version_store,
                      pending_cross_request_read_set, pushers, cct, get_client_id_to_address_map);
                  to_fetch_map.erase(key);
                } else {
                  in_preparation[key].first.insert(request.response_address());
                  covered_locally = false;
                  to_cover.insert(key);
                }
              } else if (unmerged_store.find(key) != unmerged_store.end()) {
                in_preparation[key].second[key] = unmerged_store[key];
                recursive_dependency_check(
                    key, unmerged_store[key], in_preparation, causal_cut_store,
                    unmerged_store, to_fetch_map, cover_map, client);
                if (to_fetch_map[key].size() == 0) {
                  // all dependency met
                  merge_into_causal_cut(
                      key, causal_cut_store, in_preparation, version_store,
                      pending_cross_request_read_set, pushers, cct, get_client_id_to_address_map);
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
                client.get_async(key);
              }
            }
          }
        }
        if (!covered_locally) {
          pending_cross_request_read_set[request.response_address()].read_set_ = read_set;
          pending_cross_request_read_set[request.response_address()].to_cover_set_ = to_cover;
        } else {
          pending_cross_request_read_set[request.response_address()].read_set_ = read_set;
          // decide local and remote read set
          if (!fire_remote_read_requests(pending_cross_request_read_set[request.response_address()], version_store, causal_cut_store, pushers, cct)) {
            // all local
            CausalResponse response;

            for (const Key& key : read_set) {
              CausalTuple* tp = response.add_tuples();
              tp->set_key(key);
              tp->set_payload(serialize(*(causal_cut_store[key])));
            }

            response.set_versioned_key_query_addr(cct.causal_cache_versioned_key_request_connect_address());

            for (const auto& pair : version_store[request.id()]) {
              VersionedKey* vk = response.add_versioned_keys();
              vk->set_key(pair.first);
              auto ptr = vk->mutable_vector_clock();
              for (const auto& client_version_pair : pair.second->reveal().vector_clock.reveal()) {
                (*ptr)[client_version_pair.first] = client_version_pair.second.reveal();
              }
            }

            // send response
            string resp_string;
            response.SerializeToString(&resp_string);
            kZmqUtil->send_string(resp_string,
                                  &pushers[request.response_address()]);
            pending_cross_request_read_set.erase(request.response_address());
          } else {
            get_client_id_to_address_map[request.id()].insert(request.response_address());
          }
        }
      } else {
        log->error("Found non-causal consistency level.");
      }
    }

    // handle a PUT request
    if (pollitems[1].revents & ZMQ_POLLIN) {
      string serialized = kZmqUtil->recv_string(&put_puller);
      CausalRequest request;
      request.ParseFromString(serialized);

      for (CausalTuple tuple : request.tuples()) {
        Key key = tuple.key();
        auto lattice = std::make_shared<CrossCausalLattice<SetLattice<string>>>(
            to_cross_causal_payload(deserialize_cross_causal(tuple.payload())));
        // first, update unmerged store
        if (unmerged_store.find(key) == unmerged_store.end()) {
          unmerged_store[key] = lattice;
        } else {
          unsigned comp_result =
              causal_comparison(unmerged_store[key], lattice);
          if (comp_result == kCausalLess) {
            unmerged_store[key] = lattice;
          } else if (comp_result == kCausalConcurrent) {
            unmerged_store[key] = causal_merge(unmerged_store[key], lattice);
          }
        }
        // if cross causal, also update causal cut
        if (request.consistency() == ConsistencyType::CROSS) {
          // we compare two lattices
          unsigned comp_result =
              causal_comparison(causal_cut_store[key], lattice);
          if (comp_result == kCausalLess) {
            causal_cut_store[key] = lattice;
          } else if (comp_result == kCausalConcurrent) {
            causal_cut_store[key] =
                causal_merge(causal_cut_store[key], lattice);
          }
          // keep this version
          version_store[request.id()][key] = lattice;
        }
        // write to KVS
        string req_id = client.put_async(key, serialize(*unmerged_store[key]),
                                         LatticeType::CROSSCAUSAL);
        put_request_id_to_address_map[req_id] = request.response_address();
      }
    }

    // handle updates received from the KVS
    if (pollitems[2].revents & ZMQ_POLLIN) {
      string serialized = kZmqUtil->recv_string(&update_puller);
      KeyRequest updates;
      updates.ParseFromString(serialized);

      for (const KeyTuple& tuple : updates.tuples()) {
        Key key = tuple.key();

        // if we are no longer caching this key, then we simply ignore updates
        // for it because we received the update based on outdated information
        if (key_set.find(key) == key_set.end()) {
          continue;
        }

        auto lattice = std::make_shared<CrossCausalLattice<SetLattice<string>>>(
            to_cross_causal_payload(deserialize_cross_causal(tuple.payload())));

        process_response(key, lattice, unmerged_store, in_preparation,
                         causal_cut_store, version_store, single_callback_map,
                         pending_single_request_read_set,
                         pending_cross_request_read_set, to_fetch_map,
                         cover_map, pushers, client, log, cct, get_client_id_to_address_map);
      }
    }

    // handle version GC request
    if (pollitems[3].revents & ZMQ_POLLIN) {
      // assume this string is the client id
      string serialized = kZmqUtil->recv_string(&version_gc_puller);
      version_store.erase(serialized);
    }

    // handle versioned key request
    if (pollitems[4].revents & ZMQ_POLLIN) {
      string serialized = kZmqUtil->recv_string(&versioned_key_request_puller);
      VersionedKeyRequest request;
      request.ParseFromString(serialized);

      VersionedKeyResponse response;
      response.set_id(request.id());
      if (version_store.find(request.id()) != version_store.end()) {
        for (const auto& key : request.keys()) {
          if (version_store[request.id()].find(key) ==
              version_store[request.id()].end()) {
            log->error(
                "Requested key {} for client ID {} not available in versioned "
                "store.",
                key, request.id());
          } else {
            CausalTuple* tp = response.add_tuples();
            tp->set_key(key);
            tp->set_payload(serialize(*(version_store[request.id()][key])));
          }
        }
      } else {
        log->error("Client ID {} not available in versioned store.",
                   request.id());
      }
      // send response
      string resp_string;
      response.SerializeToString(&resp_string);
      kZmqUtil->send_string(resp_string, &pushers[request.response_address()]);
    }

    // handle versioned key response
    if (pollitems[5].revents & ZMQ_POLLIN) {
      string serialized = kZmqUtil->recv_string(&versioned_key_response_puller);
      VersionedKeyResponse response;
      response.ParseFromString(serialized);

      if (get_client_id_to_address_map.find(response.id()) != get_client_id_to_address_map.end()) {
        for (const Address& addr : get_client_id_to_address_map[response.id()]) {
          if (pending_cross_request_read_set.find(addr) != pending_cross_request_read_set.end()) {
            for (const CausalTuple& tp : response.tuples()) {
              if (pending_cross_request_read_set[addr].remote_read_set_.find(tp.key()) != pending_cross_request_read_set[addr].remote_read_set_.end()) {
                pending_cross_request_read_set[addr].serialized_remote_payload_[tp.key()] = tp.payload();
                pending_cross_request_read_set[addr].remote_read_set_.erase(tp.key());
              }
            }

            if (pending_cross_request_read_set[addr].remote_read_set_.size() == 0) {
              // all remote read finished
              CausalResponse response;

              for (const auto& pair : pending_cross_request_read_set[addr].serialized_local_payload_) {
                CausalTuple* tp = response.add_tuples();
                tp->set_key(std::move(pair.first));
                tp->set_payload(std::move(pair.second));
              }

              for (const auto& pair : pending_cross_request_read_set[addr].serialized_remote_payload_) {
                CausalTuple* tp = response.add_tuples();
                tp->set_key(std::move(pair.first));
                tp->set_payload(std::move(pair.second));
              }

              response.set_versioned_key_query_addr(cct.causal_cache_versioned_key_request_connect_address());

              for (const auto& pair : version_store[pending_cross_request_read_set[addr].client_id_]) {
                VersionedKey* vk = response.add_versioned_keys();
                vk->set_key(pair.first);
                auto ptr = vk->mutable_vector_clock();
                for (const auto& client_version_pair : pair.second->reveal().vector_clock.reveal()) {
                  (*ptr)[client_version_pair.first] = client_version_pair.second.reveal();
                }
              }

              // send response
              string resp_string;
              response.SerializeToString(&resp_string);
              kZmqUtil->send_string(resp_string,
                                    &pushers[addr]);
              // GC
              pending_cross_request_read_set.erase(addr);

              // GC
              set<string> to_remove_id;
              for (auto& pair : get_client_id_to_address_map) {
                pair.second.erase(addr);

                if (pair.second.size() == 0) {
                  to_remove_id.insert(pair.first);
                }
              }

              for (const auto& id : to_remove_id) {
                get_client_id_to_address_map.erase(id);
              }
            }
          }
        }
      }
    }

    vector<KeyResponse> responses = client.receive_async(kZmqUtil);
    for (const auto& response : responses) {
      Key key = response.tuples(0).key();
      // first, check if the request failed
      if (response.response_id() == "NULL_ERROR") {
        if (response.type() == RequestType::GET) {
          client.get_async(key);
        } else {
          if (put_request_id_to_address_map.find(response.response_id()) !=
              put_request_id_to_address_map.end()) {
            // we only retry for client-issued requests, not for the periodic
            // stat report
            string new_req_id = client.put_async(
                key, response.tuples(0).payload(), LatticeType::CROSSCAUSAL);
            put_request_id_to_address_map[new_req_id] =
                put_request_id_to_address_map[response.response_id()];
            // GC the original request_id address pair
            put_request_id_to_address_map.erase(response.response_id());
          }
        }
      } else {
        if (response.type() == RequestType::GET) {
          auto lattice =
              std::make_shared<CrossCausalLattice<SetLattice<string>>>();
          if (response.tuples(0).error() != 1) {
            // key exists
            *lattice =
                CrossCausalLattice<SetLattice<string>>(to_cross_causal_payload(
                    deserialize_cross_causal(response.tuples(0).payload())));
          }
          process_response(key, lattice, unmerged_store, in_preparation,
                           causal_cut_store, version_store, single_callback_map,
                           pending_single_request_read_set,
                           pending_cross_request_read_set, to_fetch_map,
                           cover_map, pushers, client, log, cct, get_client_id_to_address_map);
        } else {
          if (put_request_id_to_address_map.find(response.response_id()) ==
              put_request_id_to_address_map.end()) {
            log->error(
                "Missing request id - address entry for this PUT response");
          } else {
            CausalResponse resp;
            CausalTuple* tp = resp.add_tuples();
            tp->set_key(key);
            string resp_string;
            resp.SerializeToString(&resp_string);
            kZmqUtil->send_string(
                resp_string,
                &pushers[put_request_id_to_address_map[response.response_id()]]);
          }
        }
      }
    }

    // collect and store internal statistics
    report_end = std::chrono::system_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::seconds>(
                        report_end - report_start)
                        .count();

    // update KVS with information about which keys this node is currently
    // caching; we only do this periodically because we are okay with receiving
    // potentially stale updates
    if (duration >= kCausalCacheReportThreshold) {
      KeySet set;

      for (const auto& pair : unmerged_store) {
        set.add_keys(pair.first);
      }

      string serialized;
      set.SerializeToString(&serialized);

      LWWPairLattice<string> val(TimestampValuePair<string>(
          generate_timestamp(thread_id), serialized));
      Key key = get_user_metadata_key(ip, UserMetadataType::cache_ip);
      client.put_async(key, serialize(val), LatticeType::LWW);
      report_start = std::chrono::system_clock::now();
    }

    migrate_end = std::chrono::system_clock::now();
    duration = std::chrono::duration_cast<std::chrono::seconds>(migrate_end -
                                                                migrate_start)
                   .count();

    // check if any key in unmerged_store is newer and migrate
    if (duration >= kMigrateThreshold) {
      for (const auto& pair : unmerged_store) {
        if ((causal_cut_store.find(pair.first) == causal_cut_store.end() ||
             causal_comparison(causal_cut_store[pair.first], pair.second) !=
                 kCausalGreaterOrEqual) &&
            find_lattice_from_in_preparation(in_preparation, pair.first) ==
                nullptr) {
          to_fetch_map[pair.first] = set<Key>();
          in_preparation[pair.first].second[pair.first] = pair.second;
          recursive_dependency_check(pair.first, pair.second, in_preparation,
                                     causal_cut_store, unmerged_store,
                                     to_fetch_map, cover_map, client);
          if (to_fetch_map[pair.first].size() == 0) {
            // all dependency met
            merge_into_causal_cut(pair.first, causal_cut_store, in_preparation,
                                  version_store, pending_cross_request_read_set,
                                  pushers, cct, get_client_id_to_address_map);
            to_fetch_map.erase(pair.first);
          }
        }
      }
      migrate_start = std::chrono::system_clock::now();
    }

    // TODO: check if cache size is exceeding (threshold x capacity) and evict.
  }
}

int main(int argc, char* argv[]) {
  if (argc > 1) {
    std::cerr << "Usage: " << argv[0] << "" << std::endl;
    return 1;
  }

  // read the YAML conf
  YAML::Node conf = YAML::LoadFile("conf/kvs-config.yml");
  unsigned kRoutingThreadCount = conf["threads"]["routing"].as<unsigned>();

  YAML::Node user = conf["user"];
  Address ip = user["ip"].as<Address>();

  vector<Address> routing_ips;
  if (YAML::Node elb = user["routing-elb"]) {
    routing_ips.push_back(elb.as<Address>());
  } else {
    YAML::Node routing = user["routing"];
    for (const YAML::Node& node : routing) {
      routing_ips.push_back(node.as<Address>());
    }
  }

  vector<UserRoutingThread> threads;
  for (Address addr : routing_ips) {
    for (unsigned i = 0; i < kRoutingThreadCount; i++) {
      threads.push_back(UserRoutingThread(addr, i));
    }
  }

  KvsAsyncClient client(threads, ip, 0, 10000);

  run(client, ip, 0);
}