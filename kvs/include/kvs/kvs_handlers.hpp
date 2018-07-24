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

#ifndef SRC_INCLUDE_KVS_KVS_HANDLERS_HPP_
#define SRC_INCLUDE_KVS_KVS_HANDLERS_HPP_

#include "hash_ring.hpp"
#include "requests.hpp"
#include "spdlog/spdlog.h"
#include "utils/server_utils.hpp"

void node_join_handler(
    unsigned thread_id, unsigned& seed, Address ip,
    std::shared_ptr<spdlog::logger> logger, std::string& serialized,
    std::unordered_map<unsigned, GlobalHashRing>& global_hash_ring_map,
    std::unordered_map<unsigned, LocalHashRing>& local_hash_ring_map,
    std::unordered_map<Key, unsigned>& key_stat_map,
    std::unordered_map<Key, KeyInfo>& placement,
    std::unordered_set<Key>& join_remove_set, SocketCache& pushers,
    ServerThread& wt, AddressKeysetMap& join_addr_keyset_map);

void node_depart_handler(
    unsigned thread_id, Address ip,
    std::unordered_map<unsigned, GlobalHashRing>& global_hash_ring_map,
    std::shared_ptr<spdlog::logger> logger, std::string& serialized,
    SocketCache& pushers);

void self_depart_handler(
    unsigned thread_id, unsigned& seed, Address ip,
    std::shared_ptr<spdlog::logger> logger, std::string& serialized,
    std::unordered_map<unsigned, GlobalHashRing>& global_hash_ring_map,
    std::unordered_map<unsigned, LocalHashRing>& local_hash_ring_map,
    std::unordered_map<Key, unsigned>& key_stat_map,
    std::unordered_map<Key, KeyInfo>& placement,
    std::vector<Address>& routing_address,
    std::vector<Address>& monitoring_address, ServerThread& wt,
    SocketCache& pushers, Serializer* serializer);

void user_request_handler(
    unsigned& total_access, unsigned& seed, std::string& serialized,
    std::chrono::system_clock::time_point& start_time,
    std::shared_ptr<spdlog::logger> logger,
    std::unordered_map<unsigned, GlobalHashRing>& global_hash_ring_map,
    std::unordered_map<unsigned, LocalHashRing>& local_hash_ring_map,
    std::unordered_map<std::string, unsigned>& key_size_map,
    PendingMap<PendingRequest>& pending_request_map,
    std::unordered_map<
        Key, std::multiset<std::chrono::time_point<std::chrono::system_clock>>>&
        key_access_timestamp,
    std::unordered_map<Key, KeyInfo>& placement,
    std::unordered_set<Key>& local_changeset, ServerThread& wt,
    Serializer* serializer, SocketCache& pushers);

void gossip_handler(
    unsigned& seed, std::string& serialized,
    std::unordered_map<unsigned, GlobalHashRing>& global_hash_ring_map,
    std::unordered_map<unsigned, LocalHashRing>& local_hash_ring_map,
    std::unordered_map<std::string, unsigned>& key_size_map,
    PendingMap<PendingGossip>& pending_gossip_map,
    std::unordered_map<Key, KeyInfo>& placement, ServerThread& wt,
    Serializer* serializer, SocketCache& pushers);

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
    Serializer* serializer, SocketCache& pushers);

void rep_factor_change_handler(
    Address ip, unsigned thread_id, unsigned& seed,
    std::shared_ptr<spdlog::logger> logger, std::string& serialized,
    std::unordered_map<unsigned, GlobalHashRing>& global_hash_ring_map,
    std::unordered_map<unsigned, LocalHashRing>& local_hash_ring_map,
    std::unordered_map<Key, KeyInfo>& placement,
    std::unordered_map<std::string, unsigned>& key_size_map,
    std::unordered_set<Key>& local_changeset, ServerThread& wt,
    Serializer* serializer, SocketCache& pushers);

void send_gossip(AddressKeysetMap& addr_keyset_map, SocketCache& pushers,
                 Serializer* serializer);

std::pair<ReadCommittedPairLattice<std::string>, unsigned> process_get(
    const Key& key, Serializer* serializer);

void process_put(const Key& key, const unsigned long long& timestamp,
                 const std::string& value, Serializer* serializer,
                 std::unordered_map<std::string, unsigned>& key_size_map);

bool is_primary_replica(
    const Key& key, std::unordered_map<Key, KeyInfo>& placement,
    std::unordered_map<unsigned, GlobalHashRing>& global_hash_ring_map,
    std::unordered_map<unsigned, LocalHashRing>& local_hash_ring_map,
    ServerThread& st);

#endif  // SRC_INCLUDE_KVS_KVS_HANDLERS_HPP_
