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

#ifndef SRC_INCLUDE_ROUTE_ROUTING_HANDLERS_HPP_
#define SRC_INCLUDE_ROUTE_ROUTING_HANDLERS_HPP_

#include "hash_ring.hpp"
#include "spdlog/spdlog.h"

std::string seed_handler(
    std::shared_ptr<spdlog::logger> logger,
    std::unordered_map<unsigned, GlobalHashRing>& global_hash_ring_map,
    unsigned long long duration);

void membership_handler(
    std::shared_ptr<spdlog::logger> logger, std::string& serialized,
    SocketCache& pushers,
    std::unordered_map<unsigned, GlobalHashRing>& global_hash_ring_map,
    unsigned thread_id, Address ip);

void replication_response_handler(
    std::shared_ptr<spdlog::logger> logger, std::string& serialized,
    SocketCache& pushers, RoutingThread& rt,
    std::unordered_map<unsigned, GlobalHashRing>& global_hash_ring_map,
    std::unordered_map<unsigned, LocalHashRing>& local_hash_ring_map,
    std::unordered_map<Key, KeyInfo>& placement,
    PendingMap<std::pair<Address, std::string>>& pending_key_request_map,
    unsigned& seed);

void replication_change_handler(std::shared_ptr<spdlog::logger> logger,
                                std::string& serialized, SocketCache& pushers,
                                std::unordered_map<Key, KeyInfo>& placement,
                                unsigned thread_id, Address ip);

void address_handler(
    std::shared_ptr<spdlog::logger> logger, std::string& serialized,
    SocketCache& pushers, RoutingThread& rt,
    std::unordered_map<unsigned, GlobalHashRing>& global_hash_ring_map,
    std::unordered_map<unsigned, LocalHashRing>& local_hash_ring_map,
    std::unordered_map<Key, KeyInfo>& placement,
    PendingMap<std::pair<Address, std::string>>& pending_key_request_map,
    unsigned& seed);

#endif  // SRC_INCLUDE_ROUTE_ROUTING_HANDLERS_HPP_
