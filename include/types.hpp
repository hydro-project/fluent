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

#ifndef INCLUDE_TYPES_HPP_
#define INCLUDE_TYPES_HPP_

#include <set>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>
#include "spdlog/spdlog.h"

using string = std::string;

template <class K, class V>
using map = std::unordered_map<K, V>;

template <class T>
using ordered_set = std::set<T>;

template <class T>
using set = std::unordered_set<T>;

template <class T>
using vector = std::vector<T>;

template <class F, class S>
using pair = std::pair<F, S>;

using Address = std::string;

using Key = std::string;

using logger = std::shared_ptr<spdlog::logger>;

#endif  // INCLUDE_TYPES_HPP_
