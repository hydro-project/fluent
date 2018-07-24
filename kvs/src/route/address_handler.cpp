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

#include "route/routing_handlers.hpp"

void address_handler(
    std::shared_ptr<spdlog::logger> logger, std::string& serialized,
    SocketCache& pushers, RoutingThread& rt,
    std::unordered_map<unsigned, GlobalHashRing>& global_hash_ring_map,
    std::unordered_map<unsigned, LocalHashRing>& local_hash_ring_map,
    std::unordered_map<Key, KeyInfo>& placement,
    PendingMap<std::pair<Address, std::string>>& pending_key_request_map,
    unsigned& seed) {
  logger->info("Received key address request.");
  KeyAddressRequest addr_request;
  addr_request.ParseFromString(serialized);

  KeyAddressResponse addr_response;
  addr_response.set_response_id(addr_request.request_id());
  bool succeed;

  int num_servers = 0;
  for (const auto& global_pair : global_hash_ring_map) {
    num_servers += global_pair.second.size();
  }

  bool respond = false;

  if (num_servers == 0) {
    addr_response.set_error(1);
    respond = true;
  } else {  // if there are servers, attempt to return the correct threads
    for (const Key& key : addr_request.keys()) {
      unsigned tier_id = 1;
      ServerThreadSet threads = {};

      while (threads.size() == 0 && tier_id < kMaxTier) {
        threads = kHashRingUtil->get_responsible_threads(
            rt.get_replication_factor_connect_addr(), key, false,
            global_hash_ring_map, local_hash_ring_map, placement, pushers,
            {tier_id}, succeed, seed);

        if (!succeed) {  // this means we don't have the replication factor for
                         // the key
          pending_key_request_map[key].push_back(
              std::pair<Address, std::string>(addr_request.response_address(),
                                              addr_request.request_id()));
          return;
        }

        tier_id++;
      }

      KeyAddressResponse_KeyAddress* tp = addr_response.add_addresses();
      tp->set_key(key);
      respond = true;
      addr_response.set_error(0);

      for (const ServerThread& thread : threads) {
        tp->add_ips(thread.get_request_pulling_connect_addr());
      }
    }
  }

  if (respond) {
    std::string serialized;
    addr_response.SerializeToString(&serialized);

    kZmqUtil->send_string(serialized,
                          &pushers[addr_request.response_address()]);
  }
}
