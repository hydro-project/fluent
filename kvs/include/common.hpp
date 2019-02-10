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

#ifndef SRC_INCLUDE_COMMON_HPP_
#define SRC_INCLUDE_COMMON_HPP_

#include <algorithm>
#include <string>
#include <unordered_map>
#include <vector>

#include "kvs_types.hpp"
#include "misc.pb.h"
#include "replication.pb.h"
#include "requests.pb.h"
#include "types.hpp"
#include "zmq/socket_cache.hpp"
#include "zmq/zmq_util.hpp"
#include "kvs/lww_pair_lattice.hpp"

const std::string kMetadataIdentifier = "ANNA_METADATA";

const unsigned kMetadataReplicationFactor = 1;
const unsigned kMetadataLocalReplicationFactor = 1;

const unsigned kVirtualThreadNum = 3000;

const unsigned kMinTier = 1;
const unsigned kMaxTier = 2;
const std::vector<unsigned> kAllTierIds = {1, 2};

const unsigned kSloWorst = 3000;
const unsigned SLO_BEST = 1500;

// run-time constants
extern unsigned kSelfTierId;
extern std::vector<unsigned> kSelfTierIdVector;

extern unsigned kMemoryNodeCapacity;
extern unsigned kEbsNodeCapacity;

// the number of threads running in this executable
extern unsigned kThreadNum;
extern unsigned kMemoryThreadCount;
extern unsigned kEbsThreadCount;
extern unsigned kRoutingThreadCount;

extern unsigned kDefaultGlobalMemoryReplication;
extern unsigned kDefaultGlobalEbsReplication;
extern unsigned kDefaultLocalReplication;
extern unsigned kMinimumReplicaNumber;

inline void split(const std::string& s, char delim,
                  std::vector<std::string>& elems) {
  std::stringstream ss(s);
  std::string item;

  while (std::getline(ss, item, delim)) {
    elems.push_back(item);
  }
}

// form the timestamp given a time and a thread id
inline unsigned long long generate_timestamp(unsigned long long time,
                                             unsigned tid) {
  unsigned pow = 10;
  while (tid >= pow) pow *= 10;
  return time * pow + tid;
}

inline void prepare_get_tuple(KeyRequest& req, Key key) {
  KeyTuple* tp = req.add_tuples();
  tp->set_key(key);
}

inline void prepare_put_tuple(KeyRequest& req, Key key, std::string payload) {

  KeyTuple* tp = req.add_tuples();
  tp->set_key(key);
  tp->set_payload(payload);
}

// TODO(vikram): what's the right way to check if this succeeded or not?
inline RequestType get_request_type(const std::string& type_str) {
  RequestType type;
  RequestType_Parse(type_str, &type);

  return type;
}

inline std::string serialize(const LWWPairLattice<std::string>& l) {
  LWWValue lww_value;
  lww_value.set_timestamp(l.reveal().timestamp);
  lww_value.set_value(l.reveal().value);
  std::string serialized;
  lww_value.SerializeToString(&serialized);
  return serialized;
}

inline std::string serialize(const unsigned long long& timestamp, const std::string& value) {
  LWWValue lww_value;
  lww_value.set_timestamp(timestamp);
  lww_value.set_value(value);
  std::string serialized;
  lww_value.SerializeToString(&serialized);
  return serialized;
}

inline std::string serialize(const SetLattice<std::string>& l) {
  SetValue set_value;
  for (const std::string& val : l.reveal()) {
    set_value.add_values(val);
  }
  std::string serialized;
  set_value.SerializeToString(&serialized);
  return serialized;
}

#endif  // SRC_INCLUDE_COMMON_HPP_
