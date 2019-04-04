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

#include "causal.pb.h"
#include "kvs_async_client.hpp"
#include "yaml-cpp/yaml.h"

ZmqUtil zmq_util;
ZmqUtilInterface* kZmqUtil = &zmq_util;

unsigned kLocalStoreReportThreshold = 5;
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
};

struct VectorClockHash {
  std::size_t operator()(const VC& vc) const {
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
  VC lhs_prev_vc = lhs->reveal().vector_clock;
  VC lhs_vc = lhs->reveal().vector_clock;
  VC rhs_vc = rhs->reveal().vector_clock;
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
unsigned vc_comparison(const VC& lhs, const VC& rhs) {
  VC lhs_prev_vc = lhs;
  VC lhs_vc = lhs;
  VC rhs_vc = rhs;
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

/*Key find_key_from_in_preparation(const InPreparationType& in_preparation,
const Key& key, bool& succeed) { for (const auto& pair : in_preparation) { for
(const auto& inner_pair : pair.second.second) { if (inner_pair.first == key) {
        succeed = true;
        return pair.first;
      }
    }
  }
  return "";
}*/

// may be more efficient to find the smallest lattice that
// satisfies the condition...
// maybe prioritize head key search?
std::shared_ptr<CrossCausalLattice<SetLattice<string>>>
find_lattice_from_in_preparation(const InPreparationType& in_preparation,
                                 const Key& key, const VC& vc = VC()) {
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
    map<Key, std::unordered_map<VC, set<Key>, VectorClockHash>>& cover_map,
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

void save_versions(const string& id, const Key& key,
                   VersionStoreType& version_store,
                   const StoreType& causal_cut_store) {
  if (version_store[id].find(key) == version_store[id].end()) {
    version_store[id][key] = causal_cut_store.at(key);
    for (const auto& dep_key :
         causal_cut_store.at(key)->reveal().dependency.key_set().reveal()) {
      save_versions(id, dep_key, version_store, causal_cut_store);
    }
  }
}

void merge_into_causal_cut(
    const Key& key, StoreType& causal_cut_store,
    InPreparationType& in_preparation, VersionStoreType& version_store,
    map<Address, PendingClientMetadata>& pending_cross_request_read_set,
    SocketCache& pushers) {
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
        CausalResponse response;
        for (const Key& key_to_read :
             pending_cross_request_read_set[addr].read_set_) {
          CausalTuple* tp = response.add_tuples();
          tp->set_key(key_to_read);
          tp->set_payload(serialize(*(causal_cut_store[key_to_read])));
          // copy pointer to keep the version
          save_versions(pending_cross_request_read_set[addr].client_id_,
                        key_to_read, version_store, causal_cut_store);
        }

        // send response
        string resp_string;
        response.SerializeToString(&resp_string);
        kZmqUtil->send_string(resp_string, &pushers[addr]);
        // GC
        pending_cross_request_read_set.erase(addr);
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
    map<Key, std::unordered_map<VC, set<Key>, VectorClockHash>>& cover_map,
    SocketCache& pushers, KvsAsyncClient& client, logger log) {
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
                            pushers);
      to_fetch_map.erase(key);
    }
  }
  // then, check cover_map to see if this key covers any dependency
  if (cover_map.find(key) != cover_map.end()) {
    std::unordered_map<VC, set<Key>, VectorClockHash> to_remove;
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
                                pushers);
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

void run(KvsAsyncClient& client, Address ip, unsigned thread_id) {
  string log_file = "local_store_log_" + std::to_string(thread_id) + ".txt";
  string log_name = "local_store_log_" + std::to_string(thread_id);
  auto log = spdlog::basic_logger_mt(log_name, log_file, true);
  log->flush_on(spdlog::level::info);

  zmq::context_t* context = client.get_context();

  SocketCache pushers(context, ZMQ_PUSH);

  // keep track of keys that this local store is responsible for
  set<Key> key_set;

  StoreType unmerged_store;
  InPreparationType in_preparation;
  StoreType causal_cut_store;
  VersionStoreType version_store;

  map<Key, set<Key>> to_fetch_map;
  map<Key, std::unordered_map<VC, set<Key>, VectorClockHash>> cover_map;

  map<Key, set<Address>> single_callback_map;

  map<Address, PendingClientMetadata> pending_single_request_read_set;
  map<Address, PendingClientMetadata> pending_cross_request_read_set;

  // mapping from request id to respond address of PUT request
  map<string, Address> request_address_map;

  LocalStoreThread lst = LocalStoreThread(ip, thread_id);

  // TODO: can we find a way to make the thread classes uniform across
  // languages? or unify the python and cpp implementations; actually, mostly
  // just the user thread stuff, I think.
  zmq::socket_t get_puller(*context, ZMQ_PULL);
  get_puller.bind(lst.local_store_get_bind_address());

  zmq::socket_t put_puller(*context, ZMQ_PULL);
  put_puller.bind(lst.local_store_put_bind_address());

  zmq::socket_t update_puller(*context, ZMQ_PULL);
  update_puller.bind(lst.local_store_update_bind_address());

  zmq::socket_t version_gc_puller(*context, ZMQ_PULL);
  version_gc_puller.bind(lst.local_store_version_gc_bind_address());

  zmq::socket_t cut_transmit_request_puller(*context, ZMQ_PULL);
  cut_transmit_request_puller.bind(
      lst.local_store_cut_transmit_request_bind_address());

  zmq::socket_t versioned_key_request_puller(*context, ZMQ_PULL);
  versioned_key_request_puller.bind(
      lst.local_store_versioned_key_request_bind_address());

  vector<zmq::pollitem_t> pollitems = {
      {static_cast<void*>(get_puller), 0, ZMQ_POLLIN, 0},
      {static_cast<void*>(put_puller), 0, ZMQ_POLLIN, 0},
      {static_cast<void*>(update_puller), 0, ZMQ_POLLIN, 0},
      {static_cast<void*>(version_gc_puller), 0, ZMQ_POLLIN, 0},
      {static_cast<void*>(cut_transmit_request_puller), 0, ZMQ_POLLIN, 0},
      {static_cast<void*>(versioned_key_request_puller), 0, ZMQ_POLLIN, 0},
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
      if (request.consistency() == ConsistencyLevel::SINGLE) {
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
      } else if (request.consistency() == ConsistencyLevel::CROSS) {
        for (CausalTuple tuple : request.tuples()) {
          Key key = tuple.key();
          read_set.insert(key);
          key_set.insert(key);

          if (causal_cut_store.find(key) == causal_cut_store.end()) {
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
                      pending_cross_request_read_set, pushers);
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
                      pending_cross_request_read_set, pushers);
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
          pending_cross_request_read_set[request.response_address()] =
              PendingClientMetadata(request.id(), read_set, to_cover);
        } else {
          CausalResponse response;
          for (const Key& key : read_set) {
            CausalTuple* tp = response.add_tuples();
            tp->set_key(key);
            tp->set_payload(serialize(*(causal_cut_store[key])));
            // copy pointer to keep the version
            save_versions(request.id(), key, version_store, causal_cut_store);
          }

          // send response
          string resp_string;
          response.SerializeToString(&resp_string);
          kZmqUtil->send_string(resp_string,
                                &pushers[request.response_address()]);
        }
      } else {
        log->error("Unrecognized consistency level.");
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
        if (request.consistency() == ConsistencyLevel::CROSS) {
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
        request_address_map[req_id] = request.response_address();
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
                         cover_map, pushers, client, log);
      }
    }

    // handle version GC request
    if (pollitems[3].revents & ZMQ_POLLIN) {
      // assume this string is the client id
      string serialized = kZmqUtil->recv_string(&version_gc_puller);
      version_store.erase(serialized);
    }

    // handle cut transmit request
    if (pollitems[4].revents & ZMQ_POLLIN) {
      string serialized = kZmqUtil->recv_string(&cut_transmit_request_puller);
      CutTransmitRequest request;
      request.ParseFromString(serialized);

      CutTransmitResponse response;
      response.set_id(request.id());
      response.set_local_store_address(
          lst.local_store_versioned_key_request_connect_address());
      if (version_store.find(request.id()) != version_store.end()) {
        for (const auto& pair : version_store[request.id()]) {
          VersionedKey* versioned_key = response.add_versioned_keys();
          versioned_key->set_key(pair.first);
          auto vc_ptr = versioned_key->mutable_vector_clock();
          for (const auto& vc_pair :
               pair.second->reveal().vector_clock.reveal()) {
            (*vc_ptr)[vc_pair.first] = vc_pair.second.reveal();
          }
        }
      }
      // send response
      string resp_string;
      response.SerializeToString(&resp_string);
      kZmqUtil->send_string(resp_string, &pushers[request.response_address()]);
    }

    // handle versioned key request
    if (pollitems[5].revents & ZMQ_POLLIN) {
      string serialized = kZmqUtil->recv_string(&versioned_key_request_puller);
      VersionedKeyRequest request;
      request.ParseFromString(serialized);

      VersionedKeyResponse response;
      response.set_id(request.id());
      response.set_local_store_address(
          lst.local_store_versioned_key_request_connect_address());
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

    vector<KeyResponse> responses = client.receive_async(kZmqUtil);
    for (const auto& response : responses) {
      Key key = response.tuples(0).key();
      // first, check if the request failed
      if (response.response_id() == "NULL_ERROR") {
        if (response.type() == RequestType::GET) {
          client.get_async(key);
        } else {
          if (request_address_map.find(response.response_id()) !=
              request_address_map.end()) {
            // we only retry for client-issued requests, not for the periodic
            // stat report
            string new_req_id = client.put_async(
                key, response.tuples(0).payload(), LatticeType::CROSSCAUSAL);
            request_address_map[new_req_id] =
                request_address_map[response.response_id()];
            // GC the original request_id address pair
            request_address_map.erase(response.response_id());
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
                           cover_map, pushers, client, log);
        } else {
          if (request_address_map.find(response.response_id()) ==
              request_address_map.end()) {
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
                &pushers[request_address_map[response.response_id()]]);
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
    if (duration >= kLocalStoreReportThreshold) {
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
                                  pushers);
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
