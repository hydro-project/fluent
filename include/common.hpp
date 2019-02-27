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

#ifndef INCLUDE_COMMON_HPP_
#define INCLUDE_COMMON_HPP_

#include <algorithm>

#include "kvs.pb.h"
#include "lattices/lww_pair_lattice.hpp"
#include "types.hpp"
#include "zmq/socket_cache.hpp"
#include "zmq/zmq_util.hpp"

inline void split(const string& s, char delim, vector<string>& elems) {
  std::stringstream ss(s);
  string item;

  while (std::getline(ss, item, delim)) {
    elems.push_back(item);
  }
}

// form the timestamp given a time and a thread id
inline unsigned long long get_time() {
  return std::chrono::duration_cast<std::chrono::milliseconds>(
             std::chrono::system_clock::now().time_since_epoch())
      .count();
}

inline unsigned long long generate_timestamp(const unsigned& id) {
  unsigned pow = 10;
  auto time = get_time();
  while (id >= pow) pow *= 10;
  return time * pow + id;
}

inline void prepare_get_tuple(KeyRequest& req, Key key,
                              LatticeType lattice_type) {
  KeyTuple* tp = req.add_tuples();
  tp->set_key(std::move(key));
  tp->set_lattice_type(std::move(lattice_type));
}

inline void prepare_put_tuple(KeyRequest& req, Key key,
                              LatticeType lattice_type, string payload) {
  KeyTuple* tp = req.add_tuples();
  tp->set_key(std::move(key));
  tp->set_lattice_type(std::move(lattice_type));
  tp->set_payload(std::move(payload));
}

inline string serialize(const LWWPairLattice<string>& l) {
  LWWValue lww_value;
  lww_value.set_timestamp(l.reveal().timestamp);
  lww_value.set_value(l.reveal().value);

  string serialized;
  lww_value.SerializeToString(&serialized);
  return serialized;
}

inline string serialize(const unsigned long long& timestamp,
                        const string& value) {
  LWWValue lww_value;
  lww_value.set_timestamp(timestamp);
  lww_value.set_value(value);

  string serialized;
  lww_value.SerializeToString(&serialized);
  return serialized;
}

inline string serialize(const SetLattice<string>& l) {
  SetValue set_value;
  for (const string& val : l.reveal()) {
    set_value.add_values(val);
  }

  string serialized;
  set_value.SerializeToString(&serialized);
  return serialized;
}

inline string serialize(const set<string>& set) {
  SetValue set_value;
  for (const string& val : set) {
    set_value.add_values(val);
  }

  string serialized;
  set_value.SerializeToString(&serialized);
  return serialized;
}

inline LWWPairLattice<string> deserialize_lww(const string& serialized) {
  LWWValue lww;
  lww.ParseFromString(serialized);

  return LWWPairLattice<string>(TimestampValuePair<string>(lww.timestamp(), lww.value()));
}

inline SetLattice<string> deserialize_set(const string& serialized) {
  SetValue s;
  s.ParseFromString(serialized);

  set<string> result;

  for (const string& value : s.values()) {
    result.insert(value);
  }

  return SetLattice<string>(result);
}

struct lattice_type_hash {
  std::size_t operator()(const LatticeType& lt) const {
    return std::hash<string>()(LatticeType_Name(lt));
  }
};

#endif  // INCLUDE_COMMON_HPP_
