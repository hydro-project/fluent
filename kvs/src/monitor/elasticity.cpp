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

#include "monitor/monitoring_utils.hpp"

void add_node(std::shared_ptr<spdlog::logger> logger, std::string tier,
              unsigned number, unsigned& adding,
              const Address& management_address) {
  logger->info("Adding {} {} node.", std::to_string(number), tier);
  std::string shell_command = "curl -X POST http://" + management_address +
                              "/add/" + tier + "/" + std::to_string(number) +
                              " &";
  system(shell_command.c_str());
  adding = number;
}

void remove_node(std::shared_ptr<spdlog::logger> logger, ServerThread& node,
                 std::string tier, bool& removing_flag, SocketCache& pushers,
                 std::unordered_map<Address, unsigned>& departing_node_map,
                 MonitoringThread& mt) {
  auto connection_addr = node.get_self_depart_connect_addr();
  departing_node_map[node.get_ip()] = kTierDataMap[1].thread_number_;
  auto ack_addr = mt.get_depart_done_connect_addr();
  logger->info("Removing {} node {}.", tier, node.get_ip());
  kZmqUtil->send_string(ack_addr, &pushers[connection_addr]);
  removing_flag = true;
}
