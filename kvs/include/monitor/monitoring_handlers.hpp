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

void membership_handler(logger log, string& serialized,
                        map<TierId, GlobalHashRing>& global_hash_rings,
                        unsigned& adding_memory_node, unsigned& adding_ebs_node,
                        TimePoint& grace_start,
                        vector<Address>& routing_address,
                        StorageStats& memory_tier_storage,
                        StorageStats& ebs_tier_storage,
                        OccupancyStats& memory_tier_occupancy,
                        OccupancyStats& ebs_tier_occupancy,
                        map<Key, map<Address, unsigned>>& key_access_frequency);

void depart_done_handler(logger log, string& serialized,
                         map<Address, unsigned>& departing_node_map,
                         Address management_address, bool& removing_memory_node,
                         bool& removing_ebs_node, SocketCache& pushers,
                         TimePoint& grace_start);

void feedback_handler(
    string& serialized, map<string, double>& user_latency,
    map<string, double>& user_throughput,
    map<Key, std::pair<double, unsigned>>& latency_miss_ratio_map);

#endif  // SRC_INCLUDE_MONITOR_MONITORING_HANDLERS_HPP_
