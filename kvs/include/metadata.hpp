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

#ifndef SRC_INCLUDE_METADATA_HPP_
#define SRC_INCLUDE_METADATA_HPP_

#include "threads.hpp"

const std::string kMetadataDelimiter = "|";
const char kMetadataDelimiterChar = '|';

// represents the replication state for each key
struct KeyInfo {
  std::unordered_map<unsigned, unsigned> global_replication_map_;
  std::unordered_map<unsigned, unsigned> local_replication_map_;
};

// per-tier metadata
struct TierData {
  TierData() : thread_number_(1), default_replication_(1), node_capacity_(0) {}

  TierData(unsigned t_num, unsigned rep, unsigned long long node_capacity) :
      thread_number_(t_num),
      default_replication_(rep),
      node_capacity_(node_capacity) {}

  unsigned thread_number_;

  unsigned default_replication_;

  unsigned long long node_capacity_;
};

inline bool is_metadata(Key key) {
  std::vector<std::string> v;
  split(key, '|', v);

  if (v[0] == kMetadataIdentifier) {
    return true;
  } else {
    return false;
  }
}

// NOTE: This needs to be here because it needs the definition of TierData
extern std::unordered_map<unsigned, TierData> kTierDataMap;

enum MetadataType { replication, server_stats, key_access, key_size };

inline Key get_metadata_key(const ServerThread& st, unsigned tier_id,
                            unsigned thread_num, MetadataType type) {
  std::string suffix;

  switch (type) {
    case MetadataType::server_stats: suffix = "stats"; break;
    case MetadataType::key_access: suffix = "access"; break;
    case MetadataType::key_size: suffix = "size"; break;
    default:
      return "";  // this should never happen; see note below about
                  // MetadataType::replication
  }

  return kMetadataIdentifier + kMetadataDelimiter + st.get_public_ip() +
         kMetadataDelimiter + st.get_private_ip() + kMetadataDelimiter +
         std::to_string(thread_num) + kMetadataDelimiter + "tier" +
         std::to_string(tier_id) + kMetadataDelimiter + suffix;
}

// This version of the function should only be called with
// MetadataType::replication, so if it's called with something else, we return
// an empty string.
// NOTE: There should probably be a less silent error check.
inline Key get_metadata_key(std::string data_key, MetadataType type) {
  if (type == MetadataType::replication) {
    return kMetadataIdentifier + kMetadataDelimiter + data_key +
           kMetadataDelimiter + "replication";
  }

  return "";
}

inline Key get_key_from_metadata(Key metadata_key) {
  std::vector<std::string> tokens;
  split(metadata_key, '|', tokens);

  if (tokens[tokens.size() - 1] == "replication") {
    return tokens[1];
  }

  return "";
}

inline std::vector<std::string> split_metadata_key(Key key) {
  std::vector<std::string> tokens;
  split(key, kMetadataDelimiterChar, tokens);

  return tokens;
}

inline void warmup_placement_to_defaults(
    std::unordered_map<Key, KeyInfo>& placement,
    unsigned& kDefaultGlobalMemoryReplication,
    unsigned& kDefaultGlobalEbsReplication,
    unsigned& kDefaultLocalReplication) {
  for (unsigned i = 1; i <= 1000000; i++) {
    // key is 8 bytes
    Key key =
        std::string(8 - std::to_string(i).length(), '0') + std::to_string(i);
    placement[key].global_replication_map_[1] = kDefaultGlobalMemoryReplication;
    placement[key].global_replication_map_[2] = kDefaultGlobalEbsReplication;
    placement[key].local_replication_map_[1] = kDefaultLocalReplication;
    placement[key].local_replication_map_[2] = kDefaultLocalReplication;
  }
}

inline void init_replication(std::unordered_map<Key, KeyInfo>& placement,
                             const Key& key) {
  for (const unsigned& tier_id : kAllTierIds) {
    placement[key].global_replication_map_[tier_id] =
        kTierDataMap[tier_id].default_replication_;
    if (tier_id != 3) {
      placement[key].local_replication_map_[tier_id] = kDefaultLocalReplication;
    }
  }
}

#endif  // SRC_INCLUDE_METADATA_HPP_
