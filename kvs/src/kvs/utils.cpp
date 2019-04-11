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
                 SerializerMap& serializers,
                 map<Key, KeyProperty>& stored_key_map) {
  map<Address, KeyRequest> gossip_map;

  for (const auto& key_pair : addr_keyset_map) {
    string address = key_pair.first;
    RequestType type;
    RequestType_Parse("PUT", &type);
    gossip_map[address].set_type(type);

    for (const auto& key : key_pair.second) {
      LatticeType type;
      if (stored_key_map.find(key) == stored_key_map.end()) {
        // we don't have this key stored, so skip
        continue;
      } else {
        type = stored_key_map[key].type_;
      }

      auto res = process_get(key, serializers[type]);

      if (res.second == 0) {
        prepare_put_tuple(gossip_map[address], key, type, res.first);
      }
    }
  }

  // send gossip
  for (const auto& gossip_pair : gossip_map) {
    string serialized;
    gossip_pair.second.SerializeToString(&serialized);
    kZmqUtil->send_string(serialized, &pushers[gossip_pair.first]);
  }
}

std::pair<string, unsigned> process_get(const Key& key,
                                        Serializer* serializer) {
  unsigned err_number = 0;
  auto res = serializer->get(key, err_number);
  return std::pair<string, unsigned>(std::move(res), err_number);
}

void process_put(const Key& key, LatticeType lattice_type,
                 const string& payload, Serializer* serializer,
                 map<Key, KeyProperty>& stored_key_map) {
  stored_key_map[key].size_ = serializer->put(key, payload);
  stored_key_map[key].type_ = std::move(lattice_type);
}

bool is_primary_replica(const Key& key,
                        map<Key, KeyReplication>& key_replication_map,
                        map<TierId, GlobalHashRing>& global_hash_rings,
                        map<TierId, LocalHashRing>& local_hash_rings,
                        ServerThread& st) {
  if (key_replication_map[key].global_replication_[kSelfTierId] == 0) {
    return false;
  } else {
    if (kSelfTierId > 1) {
      bool has_upper_tier_replica = false;
      for (const unsigned& tier_id : kAllTierIds) {
        if (tier_id < kSelfTierId &&
            key_replication_map[key].global_replication_[tier_id] > 0) {
          has_upper_tier_replica = true;
        }
      }
      if (has_upper_tier_replica) {
        return false;
      }
    }
    auto global_pos = global_hash_rings[kSelfTierId].find(key);
    if (global_pos != global_hash_rings[kSelfTierId].end() &&
        st.private_ip().compare(global_pos->second.private_ip()) == 0) {
      auto local_pos = local_hash_rings[kSelfTierId].find(key);
      if (local_pos != local_hash_rings[kSelfTierId].end() &&
          st.tid() == local_pos->second.tid()) {
        return true;
      }
    }
    return false;
  }
}
