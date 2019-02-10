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

#ifndef SRC_INCLUDE_KVS_RC_PAIR_LATTICE_HPP_
#define SRC_INCLUDE_KVS_RC_PAIR_LATTICE_HPP_

#include "../lattices/core_lattices.hpp"

template <typename T>
struct TimestampValuePair {
  // MapLattice<int, MaxLattice<int>> v_map;
  unsigned long long timestamp{0};
  T value;

  TimestampValuePair<T>() {
    timestamp = 0;
    value = T();
  }

  // need this because of static cast
  TimestampValuePair<T>(const unsigned long long& a) {
    timestamp = 0;
    value = T();
  }

  TimestampValuePair<T>(const unsigned long long& ts, const T& v) {
    timestamp = ts;
    value = v;
  }
  unsigned size() {
    return value.size() + sizeof(unsigned long long);
  }
};

template <typename T>
class LWWPairLattice : public Lattice<TimestampValuePair<T>> {
 protected:
  void do_merge(const TimestampValuePair<T>& p) {
    if (p.timestamp >= this->element.timestamp) {
      this->element.timestamp = p.timestamp;
      this->element.value = p.value;
    }
  }

 public:
  LWWPairLattice() : Lattice<TimestampValuePair<T>>() {}
  LWWPairLattice(const TimestampValuePair<T>& p) :
      Lattice<TimestampValuePair<T>>(p) {}
  MaxLattice<unsigned> size() {
    return {this->element.size()};
  }
};

#endif  // SRC_INCLUDE_KVS_RC_PAIR_LATTICE_HPP_
