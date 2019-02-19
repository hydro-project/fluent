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

#include "monitor/monitoring_handlers.hpp"

void depart_done_handler(logger log, string& serialized,
                         map<Address, unsigned>& departing_node_map,
                         Address management_ip, bool& removing_memory_node,
                         bool& removing_ebs_node, SocketCache& pushers,
                         TimePoint& grace_start) {
  vector<string> tokens;
  split(serialized, '_', tokens);

  Address departed_public_ip = tokens[0];
  Address departed_private_ip = tokens[1];
  unsigned tier_id = stoi(tokens[2]);

  if (departing_node_map.find(departed_private_ip) !=
      departing_node_map.end()) {
    departing_node_map[departed_private_ip] -= 1;

    if (departing_node_map[departed_private_ip] == 0) {
      string ntype;
      if (tier_id == 1) {
        ntype = "memory";
        removing_memory_node = false;
      } else {
        ntype = "ebs";
        removing_ebs_node = false;
      }

      log->info("Removing {} node {}/{}.", ntype, departed_public_ip,
                departed_private_ip);

      string mgmt_addr = "tcp://" + management_ip + ":7001";
      string message = "remove:" + departed_private_ip + ":" + ntype;

      kZmqUtil->send_string(message, &pushers[mgmt_addr]);

      // reset grace period timer
      grace_start = std::chrono::system_clock::now();
      departing_node_map.erase(departed_private_ip);
    }
  } else {
    log->error("Missing entry in the depart done map.");
  }
}
