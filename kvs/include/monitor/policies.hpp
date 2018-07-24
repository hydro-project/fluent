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

#ifndef SRC_INCLUDE_MONITOR_POLICIES_HPP_
#define SRC_INCLUDE_MONITOR_POLICIES_HPP_

#include "hash_ring.hpp"
#include "spdlog/spdlog.h"

void storage_policy(
    std::shared_ptr<spdlog::logger> logger,
    std::unordered_map<unsigned, GlobalHashRing>& global_hash_ring_map,
    std::chrono::time_point<std::chrono::system_clock>& grace_start,
    SummaryStats& ss, unsigned& memory_node_number, unsigned& ebs_node_number,
    unsigned& adding_memory_node, unsigned& adding_ebs_node,
    bool& removing_ebs_node, Address management_address, MonitoringThread& mt,
    std::unordered_map<Address, unsigned>& departing_node_map,
    SocketCache& pushers);

void movement_policy(
    std::shared_ptr<spdlog::logger> logger,
    std::unordered_map<unsigned, GlobalHashRing>& global_hash_ring_map,
    std::unordered_map<unsigned, LocalHashRing>& local_hash_ring_map,
    std::chrono::time_point<std::chrono::system_clock>& grace_start,
    SummaryStats& ss, unsigned& memory_node_number, unsigned& ebs_node_number,
    unsigned& adding_memory_node, unsigned& adding_ebs_node,
    Address management_address, std::unordered_map<Key, KeyInfo>& placement,
    std::unordered_map<Key, unsigned>& key_access_summary,
    std::unordered_map<Key, unsigned>& key_size, MonitoringThread& mt,
    SocketCache& pushers, zmq::socket_t& response_puller,
    std::vector<Address>& routing_address, unsigned& rid);

void slo_policy(
    std::shared_ptr<spdlog::logger> logger,
    std::unordered_map<unsigned, GlobalHashRing>& global_hash_ring_map,
    std::unordered_map<unsigned, LocalHashRing>& local_hash_ring_map,
    std::chrono::time_point<std::chrono::system_clock>& grace_start,
    SummaryStats& ss, unsigned& memory_node_number,
    unsigned& adding_memory_node, bool& removing_memory_node,
    Address management_address, std::unordered_map<Key, KeyInfo>& placement,
    std::unordered_map<Key, unsigned>& key_access_summary, MonitoringThread& mt,
    std::unordered_map<Address, unsigned>& departing_node_map,
    SocketCache& pushers, zmq::socket_t& response_puller,
    std::vector<Address>& routing_address, unsigned& rid,
    std::unordered_map<Key, std::pair<double, unsigned>>&
        latency_miss_ratio_map);

#endif  // SRC_INCLUDE_MONITOR_POLICIES_HPP_
