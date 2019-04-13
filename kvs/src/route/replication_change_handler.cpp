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

void replication_change_handler(logger log, string& serialized,
                                SocketCache& pushers,
                                map<Key, KeyReplication>& key_replication_map,
                                unsigned thread_id, Address ip) {
  if (thread_id == 0) {
    // tell all worker threads about the replication factor change
    for (unsigned tid = 1; tid < kRoutingThreadCount; tid++) {
      kZmqUtil->send_string(
          serialized, &pushers[RoutingThread(ip, tid)
                                   .replication_change_connect_address()]);
    }
  }

  ReplicationFactorUpdate update;
  update.ParseFromString(serialized);

  for (const auto& key_rep : update.key_reps()) {
    Key key = key_rep.key();
    log->info("Received a replication factor change for key {}.", key);

    for (const Replication& global : key_rep.global()) {
      key_replication_map[key].global_replication_[global.tier_id()] =
          global.replication_factor();
    }

    for (const Replication& local : key_rep.local()) {
      key_replication_map[key].local_replication_[local.tier_id()] =
          local.replication_factor();
    }
  }
}
