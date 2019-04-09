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

#include "hash_ring.hpp"
#include "metadata.pb.h"
#include "replication.pb.h"
#include "requests.hpp"

// period to report to the KVS about its key set
unsigned kCausalCacheReportThreshold = 5;

// period to migrate keys from unmerged store to causal cut store
unsigned kMigrateThreshold = 10;

// macros used for vector clock comparison
unsigned kCausalGreaterOrEqual = 0;
unsigned kCausalLess = 1;
unsigned kCausalConcurrent = 2;



#endif  // FUNCTIONS_CACHE_INCLUDE_CAUSAL_CACHE_UTILS_HPP_
