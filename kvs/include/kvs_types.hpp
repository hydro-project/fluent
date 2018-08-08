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

#include <string>
#include <unordered_map>

#include "types.hpp"

template <typename T>
using PendingMap = std::unordered_map<std::string, std::vector<T>>;

using StorageStat =
    std::unordered_map<Address,
                       std::unordered_map<unsigned, unsigned long long>>;

using OccupancyStats = std::unordered_map<
    Address, std::unordered_map<unsigned, std::pair<double, unsigned>>>;

using AccessStat =
    std::unordered_map<Address, std::unordered_map<unsigned, unsigned>>;

#endif  // SRC_INCLUDE_TYPES_HPP_
