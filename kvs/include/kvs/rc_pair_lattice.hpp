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

template <typename T>
struct TimestampValuePair {
  // MapLattice<int, MaxLattice<int>> v_map;
  int timestamp{-1};
  T value;

  TimestampValuePair<T>() {
    timestamp = -1;
    value = T();
  }

  // need this because of static cast
  TimestampValuePair<T>(int a) {
    timestamp = -1;
    value = T();
  }

  TimestampValuePair<T>(int ts, T v) {
    timestamp = ts;
    value = v;
  }
};

template <typename T>
class ReadCommittedPairLattice : public Lattice<TimestampValuePair<T>> {
 protected:
  void do_merge(const TimestampValuePair<T>& p) {
    if (p.timestamp >= this->element.timestamp) {
      this->element.timestamp = p.timestamp;
      this->element.value = p.value;
    }
  }

 public:
  ReadCommittedPairLattice() : Lattice<TimestampValuePair<T>>() {}
  ReadCommittedPairLattice(const TimestampValuePair<T>& p) :
      Lattice<TimestampValuePair<T>>(p) {}

  bool merge(const TimestampValuePair<T>& p) {
    if (p.timestamp >= this->element.timestamp) {
      this->element.timestamp = p.timestamp;
      this->element.value = p.value;

      return true;
    }

    return false;
  }

  bool merge(const ReadCommittedPairLattice<T>& pl) {
    return merge(pl.reveal());
  }
};

#endif  // SRC_INCLUDE_KVS_RC_PAIR_LATTICE_HPP_
