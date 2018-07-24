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

void depart_done_handler(
    std::shared_ptr<spdlog::logger> logger, std::string& serialized,
    std::unordered_map<Address, unsigned>& departing_node_map,
    Address management_address, bool& removing_memory_node,
    bool& removing_ebs_node,
    std::chrono::time_point<std::chrono::system_clock>& grace_start) {
  std::vector<std::string> tokens;
  split(serialized, '_', tokens);

  Address departed_ip = tokens[0];
  unsigned tier_id = stoi(tokens[1]);

  if (departing_node_map.find(departed_ip) != departing_node_map.end()) {
    departing_node_map[departed_ip] -= 1;

    if (departing_node_map[departed_ip] == 0) {
      if (tier_id == 1) {
        logger->info("Removing memory node {}.", departed_ip);

        std::string shell_command = "curl -X POST http://" +
                                    management_address + "/remove/memory/" +
                                    departed_ip;
        system(shell_command.c_str());

        removing_memory_node = false;
      } else {
        logger->info("Removing ebs node {}", departed_ip);

        std::string shell_command = "curl -X POST http://" +
                                    management_address + "/remove/ebs/" +
                                    departed_ip;
        system(shell_command.c_str());

        removing_ebs_node = false;
      }

      // reset grace period timer
      grace_start = std::chrono::system_clock::now();
      departing_node_map.erase(departed_ip);
    }
  } else {
    logger->error("Missing entry in the depart done map.");
  }
}
