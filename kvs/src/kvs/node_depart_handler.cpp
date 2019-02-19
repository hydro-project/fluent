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

void node_depart_handler(unsigned thread_id, Address public_ip,
                         Address private_ip,
                         map<TierId, GlobalHashRing>& global_hash_rings,
                         std::shared_ptr<spdlog::logger> logger,
                         string& serialized, SocketCache& pushers) {
  vector<string> v;
  split(serialized, ':', v);

  unsigned tier = stoi(v[0]);
  Address departing_public_ip = v[1];
  Address departing_private_ip = v[2];
  logger->info("Received departure for node {}/{} on tier {}.",
               departing_public_ip, departing_private_ip, tier);

  // update hash ring
  global_hash_rings[tier].remove(departing_public_ip, departing_private_ip, 0);

  if (thread_id == 0) {
    // tell all worker threads about the node departure
    for (unsigned tid = 1; tid < kThreadNum; tid++) {
      kZmqUtil->send_string(serialized,
                            &pushers[ServerThread(public_ip, private_ip, tid)
                                         .get_node_depart_connect_addr()]);
    }

    for (const auto& pair : global_hash_rings) {
      logger->info("Hash ring for tier {} size is {}.", pair.first,
                   pair.second.size());
    }
  }
}
