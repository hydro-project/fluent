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

#ifndef SRC_INCLUDE_HASHERS_HPP_
#define SRC_INCLUDE_HASHERS_HPP_

#include <unordered_set>
#include "threads.hpp"

struct ThreadHash {
  std::size_t operator()(const ServerThread& st) const {
    return std::hash<std::string>{}(st.get_id());
  }
};

// TODO: This is here for now because it needs the definition of ThreadHash, but
// it seems like it should actually be in threads.hpp; that doesn't compile
// because that file gets compiled before this one (and there is a circular
// dependency between the two?)... not completely sure how to fix this
typedef std::unordered_set<ServerThread, ThreadHash> ServerThreadSet;

struct GlobalHasher {
  uint32_t operator()(const ServerThread& th) {
    // prepend a string to make the hash value different than
    // what it would be on the naked input
    return std::hash<std::string>{}("GLOBAL" + th.get_virtual_id());
  }

  uint32_t operator()(const Key& key) {
    // prepend a string to make the hash value different than
    // what it would be on the naked input
    return std::hash<std::string>{}("GLOBAL" + key);
  }

  typedef uint32_t ResultType;
};

struct LocalHasher {
  typedef std::hash<std::string>::result_type ResultType;

  ResultType operator()(const ServerThread& th) {
    return std::hash<std::string>{}(std::to_string(th.get_tid()) + "_" +
                                    std::to_string(th.get_virtual_num()));
  }

  ResultType operator()(const Key& key) {
    return std::hash<std::string>{}(key);
  }
};

#endif  // SRC_INCLUDE_HASHERS_HPP_
