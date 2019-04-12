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

#ifndef FUNCTIONS_CACHE_INCLUDE_CAUSAL_CACHE_UTILS_HPP_
#define FUNCTIONS_CACHE_INCLUDE_CAUSAL_CACHE_UTILS_HPP_

#include "functions.pb.h"
#include "kvs_async_client.hpp"

// period to report to the KVS about its key set
const unsigned kCausalCacheReportThreshold = 5;

// period to migrate keys from unmerged store to causal cut store
const unsigned kMigrateThreshold = 10;

// macros used for vector clock comparison
const unsigned kCausalGreaterOrEqual = 0;
const unsigned kCausalLess = 1;
const unsigned kCausalConcurrent = 2;

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

  PendingClientMetadata(string client_id, set<Key> read_set,
                        set<Key> to_cover_set,
                        map<Address, map<Key, VectorClock>> prior_causal_chains,
                        set<Key> future_read_set, set<Key> remote_read_set,
                        set<Key> dne_set,
                        map<Key, string> serialized_local_payload,
                        map<Key, string> serialized_remote_payload) :
      client_id_(std::move(client_id)),
      read_set_(std::move(read_set)),
      to_cover_set_(std::move(to_cover_set)),
      prior_causal_chains_(std::move(prior_causal_chains)),
      future_read_set_(std::move(future_read_set)),
      remote_read_set_(std::move(remote_read_set)),
      dne_set_(std::move(dne_set)),
      serialized_local_payload_(std::move(serialized_local_payload)),
      serialized_remote_payload_(std::move(serialized_remote_payload)) {}

  string client_id_;
  set<Key> read_set_;
  set<Key> to_cover_set_;
  map<Address, map<Key, VectorClock>> prior_causal_chains_;
  set<Key> future_read_set_;
  set<Key> remote_read_set_;
  set<Key> dne_set_;
  map<Key, string> serialized_local_payload_;
  map<Key, string> serialized_remote_payload_;

  bool operator==(const PendingClientMetadata& input) const {
    if (client_id_ == input.client_id_ && read_set_ == input.read_set_ &&
        to_cover_set_ == input.to_cover_set_ &&
        prior_causal_chains_ == input.prior_causal_chains_ &&
        future_read_set_ == input.future_read_set_ &&
        remote_read_set_ == input.remote_read_set_ &&
        dne_set_ == input.dne_set_ &&
        serialized_local_payload_ == input.serialized_local_payload_ &&
        serialized_remote_payload_ == input.serialized_remote_payload_) {
      return true;
    } else {
      return false;
    }
  }
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
    const std::shared_ptr<CrossCausalLattice<SetLattice<string>>>& rhs);

// given two vector clocks (of the same key), compare which one is bigger
unsigned vector_clock_comparison(const VectorClock& lhs,
                                 const VectorClock& rhs);

// merge two causal lattices and if concurrent, return a pointer to a new merged
// lattice note that the original input lattices are not modified
std::shared_ptr<CrossCausalLattice<SetLattice<string>>> causal_merge(
    const std::shared_ptr<CrossCausalLattice<SetLattice<string>>>& lhs,
    const std::shared_ptr<CrossCausalLattice<SetLattice<string>>>& rhs);

// find a lattice from the in_preparation store that dominates the input lattice
// return a nullptr if not found
// TODO: may be more efficient to find the smallest lattice that
// satisfies the condition...
// TODO: maybe prioritize head key search?
std::shared_ptr<CrossCausalLattice<SetLattice<string>>>
find_lattice_from_in_preparation(const InPreparationType& in_preparation,
                                 const Key& key,
                                 const VectorClock& vc = VectorClock());

// return true if this lattice is not dominated by what's already in the
// in_preparation map this helper function is used to eliminate potential
// infinite loop
bool populate_in_preparation(
    const Key& head_key, const Key& dep_key,
    const std::shared_ptr<CrossCausalLattice<SetLattice<string>>>& lattice,
    InPreparationType& in_preparation);

// recursively check if the dependency of a key is met
void recursive_dependency_check(
    const Key& head_key,
    const std::shared_ptr<CrossCausalLattice<SetLattice<string>>>& lattice,
    InPreparationType& in_preparation, const StoreType& causal_cut_store,
    const StoreType& unmerged_store, map<Key, set<Key>>& to_fetch_map,
    map<Key, std::unordered_map<VectorClock, set<Key>, VectorClockHash>>&
        cover_map,
    KvsAsyncClientInterface* client);

// check if the given vector clock is dominated by any vector clock in the
// causal chain if so, return the address of the remote cache, else return empty
// string
Address find_address(
    const Key& key, const VectorClock& vc,
    const map<Address, map<Key, VectorClock>>& prior_causal_chains);

// save the relevant versions in case future caches may need them
// observed_key is initially passed in as an empty set
// to prevent infinite loop
void save_versions(const string& id, const Key& key,
                   VersionStoreType& version_store,
                   const StoreType& causal_cut_store,
                   const set<Key>& future_read_set, set<Key>& observed_keys);

// figure out which key need to be retrieved remotely
// and fire the requests if needed
bool fire_remote_read_requests(PendingClientMetadata& metadata,
                               VersionStoreType& version_store,
                               const StoreType& causal_cut_store,
                               SocketCache& pushers,
                               const CausalCacheThread& cct);

// respond to client with keys all from the local causal cache
void respond_to_client(
    map<Address, PendingClientMetadata>& pending_cross_metadata,
    const Address& addr, const StoreType& causal_cut_store,
    const VersionStoreType& version_store, SocketCache& pushers,
    const CausalCacheThread& cct);

// merge a causal chain from in_preparation to causal cut store
// also notify clients that are waiting for the head key of the chain
void merge_into_causal_cut(
    const Key& key, StoreType& causal_cut_store,
    InPreparationType& in_preparation, VersionStoreType& version_store,
    map<Address, PendingClientMetadata>& pending_cross_metadata,
    SocketCache& pushers, const CausalCacheThread& cct,
    map<string, set<Address>>& client_id_to_address_map);

// process a GET response received from the KVS
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
    map<string, set<Address>>& client_id_to_address_map);

// construct the causal frontier from previous causal caches
// this is later used to decide what keys should be read remotely
void populate_causal_frontier(
    const Key& key, const VectorClock& vc,
    map<Key, std::unordered_set<VectorClock, VectorClockHash>>&
        causal_frontier);

#endif  // FUNCTIONS_CACHE_INCLUDE_CAUSAL_CACHE_UTILS_HPP_
