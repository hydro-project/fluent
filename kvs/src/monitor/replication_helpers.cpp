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

#include "monitor/monitoring_utils.hpp"
#include "requests.hpp"

KeyReplication create_new_replication_vector(unsigned gm, unsigned ge,
                                             unsigned lm, unsigned le) {
  KeyReplication rep;
  rep.global_replication_[kMemoryTierId] = gm;
  rep.global_replication_[kEbsTierId] = ge;
  rep.local_replication_[kMemoryTierId] = lm;
  rep.local_replication_[kEbsTierId] = le;

  return rep;
}

void prepare_replication_factor_update(
    const Key& key,
    map<Address, ReplicationFactorUpdate>& replication_factor_map,
    Address server_address, map<Key, KeyReplication>& key_replication_map) {
  ReplicationFactor* rf = replication_factor_map[server_address].add_key_reps();
  rf->set_key(key);

  for (const auto& pair : key_replication_map[key].global_replication_) {
    Replication* global = rf->add_global();
    global->set_tier_id(pair.first);
    global->set_replication_factor(pair.second);
  }

  for (const auto& pair : key_replication_map[key].local_replication_) {
    Replication* local = rf->add_local();
    local->set_tier_id(pair.first);
    local->set_replication_factor(pair.second);
  }
}

// assume the caller has the replication factor for the keys and the requests
// are valid (rep factor <= total number of nodes in a tier)
void change_replication_factor(map<Key, KeyReplication>& requests,
                               map<TierId, GlobalHashRing>& global_hash_rings,
                               map<TierId, LocalHashRing>& local_hash_rings,
                               vector<Address>& routing_ips,
                               map<Key, KeyReplication>& key_replication_map,
                               SocketCache& pushers, MonitoringThread& mt,
                               zmq::socket_t& response_puller, logger log,
                               unsigned& rid) {
  // used to keep track of the original replication factors for the requested
  // keys
  map<Key, KeyReplication> orig_key_replication_map_info;

  // store the new replication factor synchronously in storage servers
  map<Address, KeyRequest> addr_request_map;

  // form the replication factor update request map
  map<Address, ReplicationFactorUpdate> replication_factor_map;

  for (const auto& request_pair : requests) {
    Key key = request_pair.first;
    KeyReplication new_rep = request_pair.second;
    orig_key_replication_map_info[key] = key_replication_map[key];

    // don't send an update if we're not changing the metadata
    if (new_rep == key_replication_map[key]) {
      continue;
    }

    // update the metadata map
    key_replication_map[key].global_replication_ = new_rep.global_replication_;
    key_replication_map[key].local_replication_ = new_rep.local_replication_;

    // prepare data to be stored in the storage tier
    ReplicationFactor rep_data;
    rep_data.set_key(key);

    for (const auto& pair : key_replication_map[key].global_replication_) {
      Replication* global = rep_data.add_global();
      global->set_tier_id(pair.first);
      global->set_replication_factor(pair.second);
    }

    for (const auto& pair : key_replication_map[key].local_replication_) {
      Replication* local = rep_data.add_local();
      local->set_tier_id(pair.first);
      local->set_replication_factor(pair.second);
    }

    Key rep_key = get_metadata_key(key, MetadataType::replication);

    string serialized_rep_data;
    rep_data.SerializeToString(&serialized_rep_data);
    prepare_metadata_put_request(
        rep_key, serialized_rep_data, global_hash_rings[kMemoryTierId],
        local_hash_rings[kMemoryTierId], addr_request_map,
        mt.response_connect_address(), rid);
  }

  // send updates to storage nodes
  set<Key> failed_keys;
  for (const auto& request_pair : addr_request_map) {
    bool succeed;
    auto res = make_request<KeyRequest, KeyResponse>(
        request_pair.second, pushers[request_pair.first], response_puller,
        succeed);

    if (!succeed) {
      log->error("Replication factor put timed out!");

      for (const auto& tuple : request_pair.second.tuples()) {
        failed_keys.insert(get_key_from_metadata(tuple.key()));
      }
    } else {
      for (const auto& tuple : res.tuples()) {
        if (tuple.error() == 2) {
          log->error(
              "Replication factor put for key {} rejected due to incorrect "
              "address.",
              tuple.key());

          failed_keys.insert(get_key_from_metadata(tuple.key()));
        }
      }
    }
  }

  for (const auto& request_pair : requests) {
    Key key = request_pair.first;

    if (failed_keys.find(key) == failed_keys.end()) {
      for (const unsigned& tier : kAllTierIds) {
        unsigned rep = std::max(
            key_replication_map[key].global_replication_[tier],
            orig_key_replication_map_info[key].global_replication_[tier]);
        ServerThreadList threads =
            responsible_global(key, rep, global_hash_rings[tier]);

        for (const ServerThread& thread : threads) {
          prepare_replication_factor_update(
              key, replication_factor_map,
              thread.replication_change_connect_address(), key_replication_map);
        }
      }

      // form replication factor update requests for routing nodes
      for (const string& address : routing_ips) {
        prepare_replication_factor_update(
            key, replication_factor_map,
            RoutingThread(address, 0).replication_change_connect_address(),
            key_replication_map);
      }
    }
  }

  // send replication factor update to all relevant nodes
  for (const auto& rep_factor_pair : replication_factor_map) {
    string serialized_msg;
    rep_factor_pair.second.SerializeToString(&serialized_msg);
    kZmqUtil->send_string(serialized_msg, &pushers[rep_factor_pair.first]);
  }

  // restore rep factor for failed keys
  for (const string& key : failed_keys) {
    key_replication_map[key] = orig_key_replication_map_info[key];
  }
}
