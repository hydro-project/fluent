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

#ifndef SRC_INCLUDE_KVS_SHARED_KV_STORE_HPP_
#define SRC_INCLUDE_KVS_SHARED_KV_STORE_HPP_

#include <atomic>
#include "../lattices/core_lattices.hpp"

template <typename K, typename V>
class SharedKVStore{
 protected:
  AtomicMapLattice<K, V> db;
  std::atomic<int> global_lock{0};
  tbb::concurrent_unordered_map<K, std::unique_ptr<std::atomic<int>>> lock_table;
 public:
  SharedKVStore<K, V>() {}

  SharedKVStore<K, V>(AtomicMapLattice<K, V> &other) { db = other; }

  V get(const K& k, unsigned& err_number) {
    int expected_global = 0;
    while(!global_lock.compare_exchange_strong(expected_global, expected_global - 1)) {
      if (expected_global > 0) {
        expected_global = 0;
      }
    }
    auto it = lock_table.find(k);
    if (it == lock_table.end()) {
      it = lock_table.insert({k, std::unique_ptr<std::atomic<int>>(new std::atomic<int>(0))}).first;
    }
    int expected_local = 0;
    while(!it->second->compare_exchange_strong(expected_local, expected_local - 1)) {
      if (expected_local > 0) {
        expected_local = 0;
      }
    }
    if (!db.contains(k).reveal()) {
      err_number = 1;
    }
    V result = db.at(k);
    it->second->fetch_add(1);
    global_lock.fetch_add(1);
    return result;
  }

  bool put(const K &k, const V &v) {
    int expected_global = 0;
    while(!global_lock.compare_exchange_strong(expected_global, expected_global - 1)) {
      if (expected_global > 0) {
        expected_global = 0;
      }
    }
    auto it = lock_table.find(k);
    if (it == lock_table.end()) {
      it = lock_table.insert({k, std::unique_ptr<std::atomic<int>>(new std::atomic<int>(0))}).first;
    }
    int expected_local = 0;
    while(!it->second->compare_exchange_strong(expected_local, expected_local + 1)) {
      expected_local = 0;
    }
    bool result = db.at(k).merge(v);
    it->second->fetch_sub(1);
    global_lock.fetch_add(1);
    return result;
  }

  void remove(const K& k) {
    int expected_global = 0;
    while(!global_lock.compare_exchange_strong(expected_global, expected_global + 1)) {
      expected_global = 0;
    }
    db.remove(k);
    global_lock.fetch_sub(1);
  }
};

#endif  // SRC_INCLUDE_KVS_SHARED_KV_STORE_HPP_
