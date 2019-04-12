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
#include "lattices/cross_causal_lattice.hpp"
#include "lattices/lww_pair_lattice.hpp"
#include "lattices/vector_clock_pair_lattice.hpp"
#include "types.hpp"
#include "zmq/socket_cache.hpp"
#include "zmq/zmq_util.hpp"

enum UserMetadataType { cache_ip };

// TODO: split this off for kvs vs user metadata keys?
const string kMetadataIdentifier = "ANNA_METADATA";
const string kMetadataDelimiter = "|";
const char kMetadataDelimiterChar = '|';
const string kMetadataTypeCacheIP = "cache_ip";

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

// This version of the function should only be called with
// certain types of MetadataType,
// so if it's called with something else, we return
// an empty string.
// TODO: There should probably be a less silent error check.
inline Key get_user_metadata_key(string data_key, UserMetadataType type) {
  if (type == UserMetadataType::cache_ip) {
    return kMetadataIdentifier + kMetadataDelimiter + kMetadataTypeCacheIP +
           kMetadataDelimiter + data_key;
  }
  return "";
}

// Inverse of get_user_metadata_key, returning just the key itself.
// TODO: same problem as get_user_metadata_key with the metadata types.
inline Key get_key_from_user_metadata(Key metadata_key) {
  string::size_type n_id;
  string::size_type n_type;
  // Find the first delimiter; this skips over the metadata identifier.
  n_id = metadata_key.find(kMetadataDelimiter);
  // Find the second delimiter; this skips over the metadata type.
  n_type = metadata_key.find(kMetadataDelimiter, n_id + 1);
  string metadata_type = metadata_key.substr(n_id + 1, n_type);
  if (metadata_type == kMetadataTypeCacheIP) {
    return metadata_key.substr(n_type + 1);
  }

  return "";
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

inline string serialize(const CausalPairLattice<SetLattice<string>>& l) {
  CausalValue causal_value;
  auto ptr = causal_value.mutable_vector_clock();
  // serialize vector clock
  for (const auto& pair : l.reveal().vector_clock.reveal()) {
    (*ptr)[pair.first] = pair.second.reveal();
  }
  // serialize values
  for (const string& val : l.reveal().value.reveal()) {
    causal_value.add_values(val);
  }

  string serialized;
  causal_value.SerializeToString(&serialized);
  return serialized;
}

inline string serialize(const CrossCausalLattice<SetLattice<string>>& l) {
  CrossCausalValue cross_causal_value;
  auto ptr = cross_causal_value.mutable_vector_clock();
  // serialize vector clock
  for (const auto& pair : l.reveal().vector_clock.reveal()) {
    (*ptr)[pair.first] = pair.second.reveal();
  }
  // serialize dependency
  for (const auto& pair : l.reveal().dependency.reveal()) {
    auto dep = cross_causal_value.add_deps();
    dep->set_key(pair.first);
    auto vc_ptr = dep->mutable_vector_clock();
    for (const auto& vc_pair : pair.second.reveal()) {
      (*vc_ptr)[vc_pair.first] = vc_pair.second.reveal();
    }
  }
  // serialize values
  for (const string& val : l.reveal().value.reveal()) {
    cross_causal_value.add_values(val);
  }

  string serialized;
  cross_causal_value.SerializeToString(&serialized);
  return serialized;
}

inline LWWPairLattice<string> deserialize_lww(const string& serialized) {
  LWWValue lww;
  lww.ParseFromString(serialized);

  return LWWPairLattice<string>(
      TimestampValuePair<string>(lww.timestamp(), lww.value()));
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

inline CausalValue deserialize_causal(const string& serialized) {
  CausalValue causal;
  causal.ParseFromString(serialized);

  return causal;
}

inline CrossCausalValue deserialize_cross_causal(const string& serialized) {
  CrossCausalValue cross_causal;
  cross_causal.ParseFromString(serialized);

  return cross_causal;
}

inline VectorClockValuePair<SetLattice<string>> to_vector_clock_value_pair(
    const CausalValue& cv) {
  VectorClockValuePair<SetLattice<string>> p;
  for (const auto& pair : cv.vector_clock()) {
    p.vector_clock.insert(pair.first, pair.second);
  }
  for (auto& val : cv.values()) {
    p.value.insert(std::move(val));
  }
  return p;
}

inline CrossCausalPayload<SetLattice<string>> to_cross_causal_payload(
    const CrossCausalValue& ccv) {
  CrossCausalPayload<SetLattice<string>> p;
  for (const auto& pair : ccv.vector_clock()) {
    p.vector_clock.insert(pair.first, pair.second);
  }
  for (const auto& dep : ccv.deps()) {
    VectorClock vc;
    for (const auto& pair : dep.vector_clock()) {
      vc.insert(pair.first, pair.second);
    }
    p.dependency.insert(dep.key(), vc);
  }
  for (auto& val : ccv.values()) {
    p.value.insert(std::move(val));
  }
  return p;
}

struct lattice_type_hash {
  std::size_t operator()(const LatticeType& lt) const {
    return std::hash<string>()(LatticeType_Name(lt));
  }
};

#endif  // INCLUDE_COMMON_HPP_
