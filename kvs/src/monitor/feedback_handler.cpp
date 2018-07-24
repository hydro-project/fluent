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

void feedback_handler(std::string& serialized,
                      std::unordered_map<std::string, double>& user_latency,
                      std::unordered_map<std::string, double>& user_throughput,
                      std::unordered_map<Key, std::pair<double, unsigned>>&
                          latency_miss_ratio_map) {
  UserFeedback fb;
  fb.ParseFromString(serialized);

  if (fb.has_finish() && fb.finish()) {
    user_latency.erase(fb.uid());
  } else {
    // collect latency and throughput feedback
    user_latency[fb.uid()] = fb.latency();
    user_throughput[fb.uid()] = fb.throughput();

    // collect replication factor adjustment factors
    for (const auto& key_latency_pair : fb.key_latency()) {
      Key key = key_latency_pair.key();
      double observed_key_latency = key_latency_pair.latency();

      if (latency_miss_ratio_map.find(key) == latency_miss_ratio_map.end()) {
        latency_miss_ratio_map[key].first = observed_key_latency / kSloWorst;
        latency_miss_ratio_map[key].second = 1;
      } else {
        latency_miss_ratio_map[key].first =
            (latency_miss_ratio_map[key].first *
                 latency_miss_ratio_map[key].second +
             observed_key_latency / kSloWorst) /
            (latency_miss_ratio_map[key].second + 1);
        latency_miss_ratio_map[key].second += 1;
      }
    }
  }
}
