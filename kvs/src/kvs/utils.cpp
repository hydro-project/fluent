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

#include "kvs/kvs_handlers.hpp"

void send_gossip(AddressKeysetMap& addr_keyset_map, SocketCache& pushers,
                 Serializer* serializer) {
  std::unordered_map<Address, KeyRequest> gossip_map;

  for (const auto& key_pair : addr_keyset_map) {
    std::string key = key_pair.first;
    RequestType type;
    RequestType_Parse("GET", &type);
    gossip_map[key].set_type(type);

    for (const auto& address : key_pair.second) {
      auto res = process_get(address, serializer);

      if (res.second == 0) {
        prepare_put_tuple(gossip_map[key], address, res.first.reveal().value,
                          res.first.reveal().timestamp);
      }
    }
  }

  // send gossip
  for (const auto& gossip_pair : gossip_map) {
    std::string serialized;
    gossip_pair.second.SerializeToString(&serialized);
    kZmqUtil->send_string(serialized, &pushers[gossip_pair.first]);
  }
}

std::pair<ReadCommittedPairLattice<std::string>, unsigned> process_get(
    const Key& key, Serializer* serializer) {
  unsigned err_number = 0;
  auto res = serializer->get(key, err_number);

  // check if the value is an empty string
  if (res.reveal().value == "") {
    err_number = 1;
  }

  return std::pair<ReadCommittedPairLattice<std::string>, unsigned>(res,
                                                                    err_number);
}

void process_put(const Key& key, const unsigned long long& timestamp,
                 const std::string& value, Serializer* serializer,
                 std::unordered_map<Key, unsigned>& key_size_map) {
  if (serializer->put(key, value, timestamp)) {
    // update value size if the value is replaced
    key_size_map[key] = value.size();
  }
}

bool is_primary_replica(
    const Key& key, std::unordered_map<Key, KeyInfo>& placement,
    std::unordered_map<unsigned, GlobalHashRing>& global_hash_ring_map,
    std::unordered_map<unsigned, LocalHashRing>& local_hash_ring_map,
    ServerThread& st) {
  if (placement[key].global_replication_map_[kSelfTierId] == 0) {
    return false;
  } else {
    if (kSelfTierId > 1) {
      bool has_upper_tier_replica = false;
      for (const unsigned& tier_id : kAllTierIds) {
        if (tier_id < kSelfTierId &&
            placement[key].global_replication_map_[tier_id] > 0) {
          has_upper_tier_replica = true;
        }
      }
      if (has_upper_tier_replica) {
        return false;
      }
    }
    auto global_pos = global_hash_ring_map[kSelfTierId].find(key);
    if (global_pos != global_hash_ring_map[kSelfTierId].end() &&
        st.get_ip().compare(global_pos->second.get_ip()) == 0) {
      auto local_pos = local_hash_ring_map[kSelfTierId].find(key);
      if (local_pos != local_hash_ring_map[kSelfTierId].end() &&
          st.get_tid() == local_pos->second.get_tid()) {
        return true;
      }
    }
    return false;
  }
}
