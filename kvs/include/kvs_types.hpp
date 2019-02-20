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

#ifndef SRC_INCLUDE_TYPES_HPP_
#define SRC_INCLUDE_TYPES_HPP_

#include <chrono>
#include "types.hpp"

using StorageStats = map<Address, map<unsigned, unsigned long long>>;

using OccupancyStats = map<Address, map<unsigned, pair<double, unsigned>>>;

using AccessStats = map<Address, map<unsigned, unsigned>>;

using TimePoint = std::chrono::time_point<std::chrono::system_clock>;

using TierId = unsigned;

#endif  // SRC_INCLUDE_TYPES_HPP_
