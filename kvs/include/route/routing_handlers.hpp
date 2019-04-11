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

#ifndef KVS_INCLUDE_ROUTE_ROUTING_HANDLERS_HPP_
#define KVS_INCLUDE_ROUTE_ROUTING_HANDLERS_HPP_

#include "hash_ring.hpp"
#include "metadata.pb.h"
#include "replication.pb.h"

string seed_handler(logger log, map<TierId, GlobalHashRing>& global_hash_rings);

void membership_handler(logger log, string& serialized, SocketCache& pushers,
                        map<TierId, GlobalHashRing>& global_hash_rings,
                        unsigned thread_id, Address ip);

void replication_response_handler(
    logger log, string& serialized, SocketCache& pushers, RoutingThread& rt,
    map<TierId, GlobalHashRing>& global_hash_rings,
    map<TierId, LocalHashRing>& local_hash_rings,
    map<Key, KeyReplication>& key_replication_map,
    map<Key, vector<pair<Address, string>>>& pending_requests, unsigned& seed);

void replication_change_handler(logger log, string& serialized,
                                SocketCache& pushers,
                                map<Key, KeyReplication>& key_replication_map,
                                unsigned thread_id, Address ip);

void address_handler(logger log, string& serialized, SocketCache& pushers,
                     RoutingThread& rt,
                     map<TierId, GlobalHashRing>& global_hash_rings,
                     map<TierId, LocalHashRing>& local_hash_rings,
                     map<Key, KeyReplication>& key_replication_map,
                     map<Key, vector<pair<Address, string>>>& pending_requests,
                     unsigned& seed);

#endif  // KVS_INCLUDE_ROUTE_ROUTING_HANDLERS_HPP_
