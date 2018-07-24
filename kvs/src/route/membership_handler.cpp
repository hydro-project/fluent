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

void membership_handler(
    std::shared_ptr<spdlog::logger> logger, std::string& serialized,
    SocketCache& pushers,
    std::unordered_map<unsigned, GlobalHashRing>& global_hash_ring_map,
    unsigned thread_id, Address ip) {
  std::vector<std::string> v;

  split(serialized, ':', v);
  std::string type = v[0];
  unsigned tier = stoi(v[1]);
  Address new_server_ip = v[2];

  if (type == "join") {
    logger->info("Received join from server {} in tier {}.", new_server_ip,
                 std::to_string(tier));

    // update hash ring
    bool inserted = global_hash_ring_map[tier].insert(new_server_ip, 0);

    if (inserted) {
      if (thread_id == 0) {
        // gossip the new node address between server nodes to ensure
        // consistency
        for (const auto& global_pair : global_hash_ring_map) {
          unsigned tier_id = global_pair.first;
          auto hash_ring = global_pair.second;

          for (const ServerThread& st : hash_ring.get_unique_servers()) {
            // if the node is not the newly joined node, send the ip of the
            // newly joined node
            if (st.get_ip().compare(new_server_ip) != 0) {
              kZmqUtil->send_string(std::to_string(tier) + ":" + new_server_ip,
                                    &pushers[st.get_node_join_connect_addr()]);
            }
          }
        }

        // tell all worker threads about the message
        for (unsigned tid = 1; tid < kRoutingThreadCount; tid++) {
          kZmqUtil->send_string(
              serialized,
              &pushers[RoutingThread(ip, tid).get_notify_connect_addr()]);
        }
      }
    }

    for (const auto& global_pair : global_hash_ring_map) {
      logger->info("Hash ring for tier {} size is {}.",
                   std::to_string(global_pair.first),
                   std::to_string(global_pair.second.size()));
    }
  } else if (type == "depart") {
    logger->info("Received depart from server {}.", new_server_ip);
    global_hash_ring_map[tier].remove(new_server_ip, 0);

    if (thread_id == 0) {
      // tell all worker threads about the message
      for (unsigned tid = 1; tid < kRoutingThreadCount; tid++) {
        kZmqUtil->send_string(
            serialized,
            &pushers[RoutingThread(ip, tid).get_notify_connect_addr()]);
      }
    }

    for (const auto& global_pair : global_hash_ring_map) {
      logger->info("Hash ring for tier {} size is {}.",
                   std::to_string(global_pair.first),
                   std::to_string(global_pair.second.size()));
    }
  }
}
