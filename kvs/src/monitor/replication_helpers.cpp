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

KeyInfo create_new_replication_vector(unsigned gm, unsigned ge, unsigned lm,
                                      unsigned le) {
  KeyInfo vector;
  vector.global_replication_map_[1] = gm;
  vector.global_replication_map_[2] = ge;
  vector.local_replication_map_[1] = lm;
  vector.local_replication_map_[2] = le;

  return vector;
}

void prepare_replication_factor_update(
    const Key& key,
    std::unordered_map<Address, ReplicationFactorUpdate>&
        replication_factor_map,
    Address server_address, std::unordered_map<Key, KeyInfo>& placement) {
  ReplicationFactor* rf = replication_factor_map[server_address].add_key_reps();
  rf->set_key(key);

  for (const auto& rep_pair : placement[key].global_replication_map_) {
    Replication* global = rf->add_global();
    global->set_tier_id(rep_pair.first);
    global->set_replication_factor(rep_pair.second);
  }

  for (const auto& rep_pair : placement[key].local_replication_map_) {
    Replication* local = rf->add_local();
    local->set_tier_id(rep_pair.first);
    local->set_replication_factor(rep_pair.second);
  }
}

// assume the caller has the replication factor for the keys and the requests
// are valid (rep factor <= total number of nodes in a tier)
void change_replication_factor(
    std::unordered_map<Key, KeyInfo>& requests,
    std::unordered_map<unsigned, GlobalHashRing>& global_hash_ring_map,
    std::unordered_map<unsigned, LocalHashRing>& local_hash_ring_map,
    std::vector<Address>& routing_address,
    std::unordered_map<Key, KeyInfo>& placement, SocketCache& pushers,
    MonitoringThread& mt, zmq::socket_t& response_puller,
    std::shared_ptr<spdlog::logger> logger, unsigned& rid) {
  // used to keep track of the original replication factors for the requested
  // keys
  std::unordered_map<Key, KeyInfo> orig_placement_info;

  // store the new replication factor synchronously in storage servers
  std::unordered_map<Address, KeyRequest> addr_request_map;

  // form the placement request map
  std::unordered_map<Address, ReplicationFactorUpdate> replication_factor_map;

  for (const auto& request_pair : requests) {
    Key key = request_pair.first;
    orig_placement_info[key] = placement[key];

    // update the placement map
    for (const auto& rep_pair : request_pair.second.global_replication_map_) {
      placement[key].global_replication_map_[rep_pair.first] = rep_pair.second;
    }

    for (const auto& rep_pair : request_pair.second.local_replication_map_) {
      placement[key].local_replication_map_[rep_pair.first] = rep_pair.second;
    }

    // prepare data to be stored in the storage tier
    ReplicationFactor rep_data;
    rep_data.set_key(key);
    for (const auto& rep_pair : placement[key].global_replication_map_) {
      Replication* global = rep_data.add_global();
      global->set_tier_id(rep_pair.first);
      global->set_replication_factor(rep_pair.second);
    }

    for (const auto& rep_pair : placement[key].local_replication_map_) {
      Replication* local = rep_data.add_local();
      local->set_tier_id(rep_pair.first);
      local->set_replication_factor(rep_pair.second);
    }

    Key rep_key = get_metadata_key(key, MetadataType::replication);

    std::string serialized_rep_data;
    rep_data.SerializeToString(&serialized_rep_data);
    prepare_metadata_put_request(
        rep_key, serialized_rep_data, global_hash_ring_map[1],
        local_hash_ring_map[1], addr_request_map, mt, rid);
  }

  // send updates to storage nodes
  std::unordered_set<Key> failed_keys;
  for (const auto& request_pair : addr_request_map) {
    bool succeed;
    auto res = send_request<KeyRequest, KeyResponse>(
        request_pair.second, pushers[request_pair.first], response_puller,
        succeed);

    if (!succeed) {
      logger->error("Replication factor put timed out!");

      for (const auto& tuple : request_pair.second.tuples()) {
        failed_keys.insert(get_key_from_metadata(tuple.key()));
      }
    } else {
      for (const auto& tuple : res.tuples()) {
        if (tuple.error() == 2) {
          logger->error(
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
        unsigned rep =
            std::max(placement[key].global_replication_map_[tier],
                     orig_placement_info[key].global_replication_map_[tier]);
        ServerThreadSet threads =
            responsible_global(key, rep, global_hash_ring_map[tier]);

        for (const ServerThread& thread : threads) {
          prepare_replication_factor_update(
              key, replication_factor_map,
              thread.get_replication_factor_change_connect_addr(), placement);
        }
      }

      // form placement requests for routing nodes
      for (const std::string& address : routing_address) {
        prepare_replication_factor_update(
            key, replication_factor_map,
            RoutingThread(address, 0)
                .get_replication_factor_change_connect_addr(),
            placement);
      }
    }
  }

  // send placement info update to all relevant nodes
  for (const auto& rep_factor_pair : replication_factor_map) {
    std::string serialized_msg;
    rep_factor_pair.second.SerializeToString(&serialized_msg);
    kZmqUtil->send_string(serialized_msg, &pushers[rep_factor_pair.first]);
  }

  // restore rep factor for failed keys
  for (const std::string& key : failed_keys) {
    placement[key] = orig_placement_info[key];
  }
}
