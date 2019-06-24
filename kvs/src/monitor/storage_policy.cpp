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
#include "monitor/policies.hpp"

void storage_policy(logger log, map<TierId, GlobalHashRing>& global_hash_rings,
                    TimePoint& grace_start, SummaryStats& ss,
                    unsigned& memory_node_count, unsigned& ebs_node_count,
                    unsigned& new_memory_count, unsigned& new_ebs_count,
                    bool& removing_ebs_node, Address management_ip,
                    MonitoringThread& mt,
                    map<Address, unsigned>& departing_node_map,
                    SocketCache& pushers) {
  // check storage consumption and trigger elasticity if necessary
  if (kEnableElasticity) {
    if (new_memory_count == 0 && ss.required_memory_node > memory_node_count) {
      auto time_elapsed = std::chrono::duration_cast<std::chrono::seconds>(
                              std::chrono::system_clock::now() - grace_start)
                              .count();
      if (time_elapsed > kGracePeriod) {
        add_node(log, "memory", kNodeAdditionBatchSize, new_memory_count,
                 pushers, management_ip);
      }
    }

    if (kEnableTiering && new_ebs_count == 0 &&
        ss.required_ebs_node > ebs_node_count) {
      auto time_elapsed = std::chrono::duration_cast<std::chrono::seconds>(
                              std::chrono::system_clock::now() - grace_start)
                              .count();
      if (time_elapsed > kGracePeriod) {
        add_node(log, "ebs", kNodeAdditionBatchSize, new_ebs_count, pushers,
                 management_ip);
      }
    }

    if (kEnableTiering &&
        ss.avg_ebs_consumption_percentage < kMinEbsNodeConsumption &&
        !removing_ebs_node &&
        ebs_node_count >
            std::max(ss.required_ebs_node, (unsigned)kMinEbsTierSize)) {
      auto time_elapsed = std::chrono::duration_cast<std::chrono::seconds>(
                              std::chrono::system_clock::now() - grace_start)
                              .count();

      if (time_elapsed > kGracePeriod) {
        // pick a random ebs node and send remove node command
        auto node = next(global_hash_rings[kEbsTierId].begin(),
                         rand() % global_hash_rings[kEbsTierId].size())
                        ->second;
        remove_node(log, node, "ebs", removing_ebs_node, pushers,
                    departing_node_map, mt);
      }
    }
  }
}
