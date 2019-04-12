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

void versioned_key_request_handler(const string& serialized,
                                   VersionStoreType& version_store,
                                   SocketCache& pushers, logger log,
                                   ZmqUtilInterface* kZmqUtil) {
  VersionedKeyRequest request;
  request.ParseFromString(serialized);

  VersionedKeyResponse response;
  response.set_id(request.id());
  if (version_store.find(request.id()) != version_store.end()) {
    for (const auto& key : request.keys()) {
      if (version_store[request.id()].find(key) ==
          version_store[request.id()].end()) {
        log->error(
            "Requested key {} for client ID {} not available in versioned "
            "store.",
            key, request.id());
      } else {
        CausalTuple* tp = response.add_tuples();
        tp->set_key(key);
        tp->set_payload(serialize(*(version_store[request.id()][key])));
      }
    }
  } else {
    log->error("Client ID {} not available in versioned store.", request.id());
  }
  // send response
  string resp_string;
  response.SerializeToString(&resp_string);
  kZmqUtil->send_string(resp_string, &pushers[request.response_address()]);
}

void versioned_key_response_handler(
    const string& serialized, StoreType& causal_cut_store,
    VersionStoreType& version_store,
    map<Address, PendingClientMetadata>& pending_cross_metadata,
    map<string, set<Address>>& client_id_to_address_map,
    const CausalCacheThread& cct, SocketCache& pushers,
    ZmqUtilInterface* kZmqUtil) {
  VersionedKeyResponse response;
  response.ParseFromString(serialized);

  if (client_id_to_address_map.find(response.id()) !=
      client_id_to_address_map.end()) {
    for (const Address& addr : client_id_to_address_map[response.id()]) {
      if (pending_cross_metadata.find(addr) != pending_cross_metadata.end()) {
        for (const CausalTuple& tp : response.tuples()) {
          if (pending_cross_metadata[addr].remote_read_set_.find(tp.key()) !=
              pending_cross_metadata[addr].remote_read_set_.end()) {
            pending_cross_metadata[addr].serialized_remote_payload_[tp.key()] =
                tp.payload();
            pending_cross_metadata[addr].remote_read_set_.erase(tp.key());
          }
        }

        if (pending_cross_metadata[addr].remote_read_set_.size() == 0) {
          // all remote read finished
          CausalResponse response;

          for (const auto& pair :
               pending_cross_metadata[addr].serialized_local_payload_) {
            CausalTuple* tp = response.add_tuples();
            tp->set_key(std::move(pair.first));

            if (pending_cross_metadata[addr].dne_set_.find(pair.first) !=
                pending_cross_metadata[addr].dne_set_.end()) {
              // key dne
              tp->set_error(1);
            } else {
              tp->set_error(0);
              tp->set_payload(std::move(pair.second));
            }
          }

          for (const auto& pair :
               pending_cross_metadata[addr].serialized_remote_payload_) {
            CausalTuple* tp = response.add_tuples();
            tp->set_key(std::move(pair.first));
            tp->set_payload(std::move(pair.second));
          }

          response.set_versioned_key_query_addr(
              cct.causal_cache_versioned_key_request_connect_address());

          for (const auto& pair :
               version_store[pending_cross_metadata[addr].client_id_]) {
            VersionedKey* vk = response.add_versioned_keys();
            vk->set_key(pair.first);
            auto ptr = vk->mutable_vector_clock();
            for (const auto& client_version_pair :
                 pair.second->reveal().vector_clock.reveal()) {
              (*ptr)[client_version_pair.first] =
                  client_version_pair.second.reveal();
            }
          }

          // send response
          string resp_string;
          response.SerializeToString(&resp_string);
          kZmqUtil->send_string(resp_string, &pushers[addr]);
          // GC
          pending_cross_metadata.erase(addr);

          // GC
          set<string> to_remove_id;
          for (auto& pair : client_id_to_address_map) {
            pair.second.erase(addr);

            if (pair.second.size() == 0) {
              to_remove_id.insert(pair.first);
            }
          }

          for (const auto& id : to_remove_id) {
            client_id_to_address_map.erase(id);
          }
        }
      }
    }
  }
}