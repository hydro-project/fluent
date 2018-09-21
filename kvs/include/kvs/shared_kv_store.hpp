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

  void acquire_global_S() {
    int expected_global = 0;
    while(!global_lock.compare_exchange_strong(expected_global, expected_global - 1)) {
      if (expected_global > 0) {
        expected_global = 0;
      }
    }
  }

  void acquire_global_X() {
    int expected_global = 0;
    while(!global_lock.compare_exchange_strong(expected_global, expected_global + 1)) {
      expected_global = 0;
    }
  }

  void release_global_S() {
    global_lock.fetch_add(1);
  }

  void release_global_X() {
    global_lock.fetch_sub(1);
  }

  std::unique_ptr<std::atomic<int>>& acquire_local_S(const K& k) {
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
    return it->second;
  }

  std::unique_ptr<std::atomic<int>>& acquire_local_X(const K& k) {
    auto it = lock_table.find(k);
    if (it == lock_table.end()) {
      it = lock_table.insert({k, std::unique_ptr<std::atomic<int>>(new std::atomic<int>(0))}).first;
    }
    int expected_local = 0;
    while(!it->second->compare_exchange_strong(expected_local, expected_local + 1)) {
      expected_local = 0;
    }
    return it->second;
  }

  void release_local_S(std::unique_ptr<std::atomic<int>>& ptr) {
    ptr->fetch_add(1);
  }

  void release_local_X(std::unique_ptr<std::atomic<int>>& ptr) {
    ptr->fetch_sub(1);
  }
 public:
  SharedKVStore<K, V>() {}

  SharedKVStore<K, V>(AtomicMapLattice<K, V> &other) { db = other; }

  bool contains(const K& k) {
    acquire_global_S();
    auto ptr = &acquire_local_S(k);
    bool result = db.contains(k).reveal();
    release_local_S(*ptr);
    release_global_S();
    return result;
  }

  V get(const K& k, unsigned& err_number) {
    acquire_global_S();
    auto ptr = &acquire_local_S(k);
    if (!db.contains(k).reveal()) {
      err_number = 1;
    }
    V result = db.at(k);
    release_local_S(*ptr);
    release_global_S();
    return result;
  }

  bool put(const K &k, const V &v) {
    acquire_global_S();
    auto ptr = &acquire_local_X(k);
    bool result = db.at(k).merge(v);
    release_local_X(*ptr);
    release_global_S();
    return result;
  }

  void remove(const K& k) {
    acquire_global_X();
    db.remove(k);
    release_global_X();
  }

  std::unordered_set<K> key_set() {
    acquire_global_S();
    std::unordered_set<K> result = db.key_set().reveal();
    release_global_S();
    return result;
  }

  unsigned key_size(const K& k) {
    acquire_global_S();
    auto ptr = &acquire_local_S(k);
    unsigned result = db.at(k).size();
    release_local_S(*ptr);
    release_global_S();
    return result;
  }

};

#endif  // SRC_INCLUDE_KVS_SHARED_KV_STORE_HPP_
