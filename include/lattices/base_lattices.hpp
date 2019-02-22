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

#ifndef SRC_INCLUDE_LATTICES_BASE_LATTICES_HPP_
#define SRC_INCLUDE_LATTICES_BASE_LATTICES_HPP_

template <typename T>
class Lattice {
 protected:
  T element;
  const T zero{static_cast<T>(0)};
  virtual void do_merge(const T &e) = 0;

 public:
  Lattice<T>() { assign(bot()); }

  Lattice<T>(const T &e) { assign(e); }

  Lattice<T>(const Lattice<T> &other) { assign(other.reveal()); }

  virtual ~Lattice<T>() = default;
  Lattice<T> &operator=(const Lattice<T> &rhs) {
    assign(rhs.reveal());
    return *this;
  }

  bool operator==(const Lattice<T> &rhs) const {
    return this->reveal() == rhs.reveal();
  }

  const T &reveal() const { return element; }

  const T &bot() const { return zero; }

  void merge(const T &e) { return do_merge(e); }

  void merge(const Lattice<T> &e) { return do_merge(e.reveal()); }

  void assign(const T e) { element = e; }

  void assign(const Lattice<T> &e) { element = e.reveal(); }
};

#endif  // SRC_INCLUDE_LATTICES_BASE_LATTICES_HPP_
