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

#ifndef KVS_INCLUDE_KVS_COMMON_HPP_
#define KVS_INCLUDE_KVS_COMMON_HPP_

#include "adaptive_heavy_hitters.hpp"
#include "kvs_types.hpp"

const unsigned kMetadataReplicationFactor = 1;
const unsigned kMetadataLocalReplicationFactor = 1;

const unsigned kVirtualThreadNum = 3000;

const unsigned kMemoryTierId = 0;
const unsigned kEbsTierId = 1;
const unsigned kRoutingTierId = 100;

const unsigned kMaxTier = 1;
const vector<unsigned> kAllTierIds = {0, 1};

const unsigned kSloWorst = 3000;

// run-time constants
extern unsigned kSelfTierId;
extern vector<unsigned> kSelfTierIdVector;

extern unsigned kMemoryNodeCapacity;
extern unsigned kEbsNodeCapacity;

// the number of threads running in this executable
extern unsigned kThreadNum;
extern unsigned kMemoryThreadCount;
extern unsigned kEbsThreadCount;
extern unsigned kRoutingThreadCount;

extern unsigned kDefaultGlobalMemoryReplication;
extern unsigned kDefaultGlobalEbsReplication;
extern unsigned kDefaultLocalReplication;
extern unsigned kMinimumReplicaNumber;

#endif  // KVS_INCLUDE_KVS_COMMON_HPP_
