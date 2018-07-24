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

#ifndef SRC_INCLUDE_KVS_BASE_KV_STORE_HPP_
#define SRC_INCLUDE_KVS_BASE_KV_STORE_HPP_

#include "../lattices/core_lattices.hpp"

template <typename K, typename V>
class KVStore {
 protected:
  MapLattice<K, V> db;

 public:
  KVStore<K, V>() {}

  KVStore<K, V>(MapLattice<K, V>& other) { db = other; }

  V get(const K& k, unsigned& err_number) {
    if (!db.contains(k).reveal()) {
      err_number = 1;
    }
    return db.at(k);
  }

  bool put(const K& k, const V& v) { return db.at(k).merge(v); }

  void remove(const K& k) { db.remove(k); }
};

#endif  // SRC_INCLUDE_KVS_BASE_KV_STORE_HPP_
