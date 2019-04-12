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

#include "causal_cache_utils.hpp"

void kvs_response_handler(
    const KeyResponse& response, StoreType& unmerged_store,
    InPreparationType& in_preparation, StoreType& causal_cut_store,
    VersionStoreType& version_store,
    map<Key, set<Address>>& single_callback_map,
    map<Address, PendingClientMetadata>& pending_single_metadata,
    map<Address, PendingClientMetadata>& pending_cross_metadata,
    map<Key, set<Key>>& to_fetch_map,
    map<Key, std::unordered_map<VectorClock, set<Key>, VectorClockHash>>&
        cover_map,
    SocketCache& pushers, KvsAsyncClientInterface* client, logger log,
    const CausalCacheThread& cct,
    map<string, set<Address>>& client_id_to_address_map,
    map<string, Address>& request_id_to_address_map) {
  Key key = response.tuples(0).key();
  // first, check if the request failed
  if (response.has_error() && response.error() == ResponseErrorType::TIMEOUT) {
    if (response.type() == RequestType::GET) {
      client->get_async(key);
    } else {
      if (request_id_to_address_map.find(response.response_id()) !=
          request_id_to_address_map.end()) {
        // we only retry for client-issued requests, not for the periodic
        // stat report
        string new_req_id = client->put_async(key, response.tuples(0).payload(),
                                              LatticeType::CROSSCAUSAL);
        request_id_to_address_map[new_req_id] =
            request_id_to_address_map[response.response_id()];
        // GC the original request_id address pair
        request_id_to_address_map.erase(response.response_id());
      }
    }
  } else {
    if (response.type() == RequestType::GET) {
      auto lattice = std::make_shared<CrossCausalLattice<SetLattice<string>>>();
      if (response.tuples(0).error() != 1) {
        // key exists
        *lattice =
            CrossCausalLattice<SetLattice<string>>(to_cross_causal_payload(
                deserialize_cross_causal(response.tuples(0).payload())));
      }
      process_response(key, lattice, unmerged_store, in_preparation,
                       causal_cut_store, version_store, single_callback_map,
                       pending_single_metadata, pending_cross_metadata,
                       to_fetch_map, cover_map, pushers, client, log, cct,
                       client_id_to_address_map);
    } else {
      if (request_id_to_address_map.find(response.response_id()) ==
          request_id_to_address_map.end()) {
        log->error("Missing request id - address entry for this PUT response");
      } else {
        CausalResponse resp;
        CausalTuple* tp = resp.add_tuples();
        tp->set_key(key);
        string resp_string;
        resp.SerializeToString(&resp_string);
        kZmqUtil->send_string(
            resp_string,
            &pushers[request_id_to_address_map[response.response_id()]]);
        // GC
        request_id_to_address_map.erase(response.response_id());
      }
    }
  }
}