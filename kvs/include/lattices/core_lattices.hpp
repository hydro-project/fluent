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

#ifndef SRC_INCLUDE_LATTICES_CORE_LATTICES_HPP_
#define SRC_INCLUDE_LATTICES_CORE_LATTICES_HPP_

#include <unordered_map>
#include <unordered_set>

#include "base_lattices.hpp"
#include "tbb/concurrent_unordered_map.h"

class BoolLattice : public Lattice<bool> {
 protected:
  void do_merge(const bool &e) { element |= e; }

 public:
  BoolLattice() : Lattice() {}
  BoolLattice(const bool &e) : Lattice(e) {}
};

template <typename T>
class MaxLattice : public Lattice<T> {
 protected:
  void do_merge(const T &e) {
    int current = this->element;

    if (current < e) {
      this->element = e;
    }
  }

 public:
  MaxLattice() : Lattice<T>() {}
  MaxLattice(const T &e) : Lattice<T>(e) {}

  // for now, all non-merge methods are non-destructive
  MaxLattice<T> add(T n) const { return MaxLattice<T>(this->element + n); }

  MaxLattice<T> subtract(T n) const { return MaxLattice<T>(this->element - n); }
};

template <typename T>
class SetLattice : public Lattice<std::unordered_set<T>> {
 protected:
  void do_merge(const std::unordered_set<T> &e) {
    for (const T &elem : e) {
      this->element.insert(elem);
    }
  }

 public:
  SetLattice() : Lattice<std::unordered_set<T>>() {}

  SetLattice(const std::unordered_set<T> &e) :
      Lattice<std::unordered_set<T>>(e) {}

  MaxLattice<int> size() const { return MaxLattice<int>(this->element.size()); }

  void insert(const T &e) { this->element.insert(e); }

  SetLattice<T> intersect(std::unordered_set<T> s) const {
    std::unordered_set<T> res;

    for (const T &that_elem : s) {
      for (const T &this_elem : this->element) {
        if (this_elem == that_elem) res.insert(this_elem);
      }
    }

    return SetLattice<T>(res);
  }

  SetLattice<T> project(bool (*f)(T)) const {
    std::unordered_set<T> res;

    for (const T &elem : this->element) {
      if (f(elem)) res.insert(elem);
    }

    return SetLattice<T>(res);
  }
};

template <typename K, typename V>
class MapLattice : public Lattice<std::unordered_map<K, V>> {
 protected:
  void insert_pair(const K &k, const V &v) {
    auto search = this->element.find(k);
    if (search != this->element.end()) {
      static_cast<V *>(&(search->second))->merge(v);
    } else {
      // need to copy v since we will be "growing" it within the lattice
      V new_v = v;
      this->element.emplace(k, new_v);
    }
  }

  void do_merge(const std::unordered_map<K, V> &m) {
    for (const auto &pair : m) {
      this->insert_pair(pair.first, pair.second);
    }
  }

 public:
  MapLattice() : Lattice<std::unordered_map<K, V>>() {}
  MapLattice(const std::unordered_map<K, V> &m) :
      Lattice<std::unordered_map<K, V>>(m) {}
  MaxLattice<int> size() const { return this->element.size(); }

  BoolLattice contains(K k) const {
    auto it = this->element.find(k);
    if (it == this->element.end())
      return BoolLattice(false);
    else
      return BoolLattice(true);
  }

  SetLattice<K> key_set() const {
    std::unordered_set<K> res;
    for (const auto &pair : this->element) {
      res.insert(pair.first);
    }
    return SetLattice<K>(res);
  }

  V &at(K k) { return this->element[k]; }

  void remove(K k) {
    auto it = this->element.find(k);
    if (it != this->element.end()) this->element.erase(k);
  }
};

template <typename K, typename V>
class AtomicMapLattice : public Lattice<tbb::concurrent_unordered_map<K, V>> {
 protected:
  void insert_pair(const K &k, const V &v) {
    auto search = this->element.find(k);
    if (search != this->element.end()) {
      static_cast<V *>(&(search->second))->merge(v);
    } else {
      // need to copy v since we will be "growing" it within the lattice
      V new_v = v;
      // FIXME: it seems that there is a bug in tbb that fails to enable c++11 features with Clang on Linux. So we have to use insert instead of emplace for now...
      // refer to https://software.intel.com/en-us/forums/intel-threading-building-blocks/topic/591305
      this->element.insert({k, new_v});
    }
  }
  void do_merge(const tbb::concurrent_unordered_map<K, V> &m) {
    for (const auto &pair : m) {
      this->insert_pair(pair.first, pair.second);
    }
  }
 public:
  AtomicMapLattice() : Lattice<tbb::concurrent_unordered_map<K, V>>() {}
  AtomicMapLattice(const tbb::concurrent_unordered_map<K, V> &m) : Lattice<tbb::concurrent_unordered_map<K, V>>(m) {}
  MaxLattice<int> size() const {
    return this->element.size();
  }

  BoolLattice contains(K k) const {
    auto it = this->element.find(k);
    if (it == this->element.end())
      return BoolLattice(false);
    else
      return BoolLattice(true);
  }

  SetLattice<K> key_set() const {
    std::unordered_set<K> res;
    for (const auto &pair : this->element) {
      res.insert(pair.first);
    }
    return SetLattice<K>(res);
  }

  V &at(K k) {
    return this->element[k];
  }

  void remove(K k) {
    auto it = this->element.find(k);
    if (it != this->element.end()) this->element.unsafe_erase(k);
  }
};

#endif  // SRC_INCLUDE_LATTICES_CORE_LATTICES_HPP_
