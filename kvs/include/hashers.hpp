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

#ifndef KVS_INCLUDE_HASHERS_HPP_
#define KVS_INCLUDE_HASHERS_HPP_

#include <vector>
#include "threads.hpp"

struct GlobalHasher {
  uint32_t operator()(const ServerThread& th) {
    // prepend a string to make the hash value different than
    // what it would be on the naked input
    return std::hash<string>{}("GLOBAL" + th.virtual_id());
  }

  uint32_t operator()(const Key& key) {
    // prepend a string to make the hash value different than
    // what it would be on the naked input
    return std::hash<string>{}("GLOBAL" + key);
  }

  typedef uint32_t ResultType;
};

struct LocalHasher {
  typedef std::hash<string>::result_type ResultType;

  ResultType operator()(const ServerThread& th) {
    return std::hash<string>{}(std::to_string(th.tid()) + "_" +
                               std::to_string(th.virtual_num()));
  }

  ResultType operator()(const Key& key) { return std::hash<string>{}(key); }
};

#endif  // KVS_INCLUDE_HASHERS_HPP_
