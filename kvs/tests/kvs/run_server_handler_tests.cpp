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

#include <stdlib.h>
#include <vector>

#include "gtest/gtest.h"

#include "kvs.pb.h"
#include "kvs/server_utils.hpp"
#include "metadata.pb.h"
#include "replication.pb.h"
#include "types.hpp"

#include "server_handler_base.hpp"
#include "test_node_depart_handler.hpp"
#include "test_node_join_handler.hpp"
#include "test_self_depart_handler.hpp"
#include "test_user_request_handler.hpp"

unsigned kDefaultLocalReplication = 1;
unsigned kSelfTierId = kMemoryTierId;
unsigned kThreadNum = 1;

vector<unsigned> kSelfTierIdVector = {kSelfTierId};
map<TierId, TierMetadata> kTierMetadata = {};

unsigned kEbsThreadNum = 1;
unsigned kMemoryThreadNum = 1;
unsigned kRoutingThreadNum = 1;

int main(int argc, char* argv[]) {
  log_->set_level(spdlog::level::info);
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
