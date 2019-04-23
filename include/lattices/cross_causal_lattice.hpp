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

#ifndef SRC_INCLUDE_KVS_CROSS_CAUSAL_LATTICE_HPP_
#define SRC_INCLUDE_KVS_CROSS_CAUSAL_LATTICE_HPP_

#include "core_lattices.hpp"

using VectorClock = MapLattice<string, MaxLattice<unsigned>>;

template <typename T>
struct CrossCausalPayload {
  VectorClock vector_clock;
  MapLattice<Key, VectorClock> dependency;
  T value;

  CrossCausalPayload<T>() {
    vector_clock = VectorClock();
    dependency = MapLattice<Key, VectorClock>();
    value = T();
  }

  // need this because of static cast
  CrossCausalPayload<T>(unsigned) {
    vector_clock = VectorClock();
    dependency = MapLattice<Key, VectorClock>();
    value = T();
  }

  CrossCausalPayload<T>(VectorClock vc, MapLattice<Key, VectorClock> dep, T v) {
    vector_clock = vc;
    dependency = dep;
    value = v;
  }

  unsigned size() {
    unsigned dep_size = 0;
    for (const auto &pair : dependency.reveal()) {
      dep_size += pair.first.size();
      dep_size += pair.second.size().reveal() * 2 * sizeof(unsigned);
    }
    return vector_clock.size().reveal() * 2 * sizeof(unsigned) + dep_size +
           value.size().reveal();
  }
};

template <typename T>
class CrossCausalLattice : public Lattice<CrossCausalPayload<T>> {
 protected:
  void do_merge(const CrossCausalPayload<T> &p) {
    VectorClock prev = this->element.vector_clock;
    this->element.vector_clock.merge(p.vector_clock);

    if (this->element.vector_clock == p.vector_clock) {
      // incoming version is dominating
      this->element.dependency.assign(p.dependency);
      this->element.value.assign(p.value);
    } else if (!(this->element.vector_clock == prev)) {
      // versions are concurrent
      this->element.dependency.merge(p.dependency);
      this->element.value.merge(p.value);
    }
  }

 public:
  CrossCausalLattice() : Lattice<CrossCausalPayload<T>>() {}
  CrossCausalLattice(const CrossCausalPayload<T> &p) :
      Lattice<CrossCausalPayload<T>>(p) {}
  MaxLattice<unsigned> size() { return {this->element.size()}; }
};

#endif  // SRC_INCLUDE_KVS_CROSS_CAUSAL_LATTICE_HPP_
