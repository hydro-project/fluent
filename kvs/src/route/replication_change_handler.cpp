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

void replication_change_handler(std::shared_ptr<spdlog::logger> logger,
                                std::string& serialized, SocketCache& pushers,
                                std::unordered_map<Key, KeyInfo>& placement,
                                unsigned thread_id, Address ip) {
  logger->info("Received a replication factor change.");

  if (thread_id == 0) {
    // tell all worker threads about the replication factor change
    for (unsigned tid = 1; tid < kRoutingThreadCount; tid++) {
      kZmqUtil->send_string(
          serialized,
          &pushers[RoutingThread(ip, tid)
                       .get_replication_factor_change_connect_addr()]);
    }
  }

  ReplicationFactorUpdate update;
  update.ParseFromString(serialized);

  for (const auto& key_rep : update.key_reps()) {
    Key key = key_rep.key();
    // update the replication factor

    for (const Replication& global : key_rep.global()) {
      placement[key].global_replication_map_[global.tier_id()] =
          global.replication_factor();
    }

    for (const Replication& local : key_rep.local()) {
      placement[key].local_replication_map_[local.tier_id()] =
          local.replication_factor();
    }
  }
}
