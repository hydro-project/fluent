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

#ifndef SRC_INCLUDE_MONITOR_MONITORING_HANDLERS_HPP_
#define SRC_INCLUDE_MONITOR_MONITORING_HANDLERS_HPP_

#include "hash_ring.hpp"
#include "spdlog/spdlog.h"

void membership_handler(
    std::shared_ptr<spdlog::logger> logger, std::string& serialized,
    std::unordered_map<unsigned, GlobalHashRing>& global_hash_ring_map,
    unsigned& adding_memory_node, unsigned& adding_ebs_node,
    std::chrono::time_point<std::chrono::system_clock>& grace_start,
    std::vector<Address>& routing_address, StorageStat& memory_tier_storage,
    StorageStat& ebs_tier_storage, OccupancyStat& memory_tier_occupancy,
    OccupancyStat& ebs_tier_occupancy,
    std::unordered_map<Key, std::unordered_map<Address, unsigned>>&
        key_access_frequency);

void depart_done_handler(
    std::shared_ptr<spdlog::logger> logger, std::string& serialized,
    std::unordered_map<Address, unsigned>& departing_node_map,
    Address management_address, bool& removing_memory_node,
    bool& removing_ebs_node,
    std::chrono::time_point<std::chrono::system_clock>& grace_start);

void feedback_handler(std::string& serialized,
                      std::unordered_map<std::string, double>& user_latency,
                      std::unordered_map<std::string, double>& user_throughput,
                      std::unordered_map<Key, std::pair<double, unsigned>>&
                          latency_miss_ratio_map);

#endif  // SRC_INCLUDE_MONITOR_MONITORING_HANDLERS_HPP_
