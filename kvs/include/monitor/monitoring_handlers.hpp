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

#ifndef KVS_INCLUDE_MONITOR_MONITORING_HANDLERS_HPP_
#define KVS_INCLUDE_MONITOR_MONITORING_HANDLERS_HPP_

#include "hash_ring.hpp"
#include "metadata.pb.h"

void membership_handler(logger log, string& serialized,
                        map<TierId, GlobalHashRing>& global_hash_rings,
                        unsigned& new_memory_count, unsigned& new_ebs_count,
                        TimePoint& grace_start, vector<Address>& routing_ips,
                        StorageStats& memory_storage, StorageStats& ebs_storage,
                        OccupancyStats& memory_occupancy,
                        OccupancyStats& ebs_occupancy,
                        map<Key, map<Address, unsigned>>& key_access_frequency,
                        map<Key, map<Address, unsigned>>& hot_key_access_frequency,
                        map<Key, map<Address, unsigned>>& cold_key_access_frequency);

void depart_done_handler(logger log, string& serialized,
                         map<Address, unsigned>& departing_node_map,
                         Address management_ip, bool& removing_memory_node,
                         bool& removing_ebs_node, SocketCache& pushers,
                         TimePoint& grace_start);

void feedback_handler(
    string& serialized, map<string, double>& user_latency,
    map<string, double>& user_throughput,
    map<Key, std::pair<double, unsigned>>& latency_miss_ratio_map);

#endif  // KVS_INCLUDE_MONITOR_MONITORING_HANDLERS_HPP_
