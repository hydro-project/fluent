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

void membership_handler(std::shared_ptr<spdlog::logger> logger,
                        string& serialized, SocketCache& pushers,
                        map<TierId, GlobalHashRing>& global_hash_rings,
                        unsigned thread_id, Address ip) {
  vector<string> v;

  split(serialized, ':', v);
  string type = v[0];
  unsigned tier = stoi(v[1]);
  Address new_server_public_ip = v[2];
  Address new_server_private_ip = v[3];

  if (type == "join") {
    // we only read the join count if it's a join message, not if it's a depart
    // message because the latter does not send a join count
    int join_count = stoi(v[4]);
    logger->info("Received join from server {}/{} in tier {}.",
                 new_server_public_ip, new_server_private_ip,
                 std::to_string(tier));

    // update hash ring
    bool inserted = global_hash_rings[tier].insert(
        new_server_public_ip, new_server_private_ip, join_count, 0);

    if (inserted) {
      if (thread_id == 0) {
        // gossip the new node address between server nodes to ensure
        // consistency
        for (const auto& pair : global_hash_rings) {
          const GlobalHashRing hash_ring = pair.second;

          // we send a message with everything but the join because that is
          // what the server nodes expect
          // NOTE: this seems like a bit of a hack right now -- should we have
          // a less ad-hoc way of doing message generation?
          string msg = v[1] + ":" + v[2] + ":" + v[3] + ":" + v[4];

          for (const ServerThread& st : hash_ring.get_unique_servers()) {
            // if the node is not the newly joined node, send the ip of the
            // newly joined node
            if (st.get_private_ip().compare(new_server_private_ip) != 0) {
              kZmqUtil->send_string(msg,
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

    for (const auto& pair : global_hash_rings) {
      logger->info("Hash ring for tier {} size is {}.", pair.first,
                   pair.second.size());
    }
  } else if (type == "depart") {
    logger->info("Received depart from server {}/{}.", new_server_public_ip,
                 new_server_private_ip, new_server_private_ip);
    global_hash_rings[tier].remove(new_server_public_ip, new_server_private_ip,
                                   0);

    if (thread_id == 0) {
      // tell all worker threads about the message
      for (unsigned tid = 1; tid < kRoutingThreadCount; tid++) {
        kZmqUtil->send_string(
            serialized,
            &pushers[RoutingThread(ip, tid).get_notify_connect_addr()]);
      }
    }

    for (int i = 0; i < global_hash_rings.size(); i++) {
      logger->info("Hash ring for tier {} size is {}.", i,
                   global_hash_rings[i].size());
    }
  }
}
