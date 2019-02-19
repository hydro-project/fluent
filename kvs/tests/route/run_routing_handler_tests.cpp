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

#include <stdio.h>
#include <stdlib.h>
#include <iostream>
#include <vector>

#include "gtest/gtest.h"
#include "spdlog/spdlog.h"

#include "misc.pb.h"
#include "replication.pb.h"
#include "requests.pb.h"
#include "types.hpp"
#include "utils/server_utils.hpp"

#include "routing_handler_base.hpp"
#include "test_address_handler.hpp"
#include "test_membership_handler.hpp"
#include "test_replication_change_handler.hpp"
#include "test_replication_response_handler.hpp"
#include "test_seed_handler.hpp"

unsigned kDefaultLocalReplication = 1;
unsigned kDefaultGlobalMemoryReplication = 1;
unsigned kDefaultGlobalEbsReplication = 1;
unsigned kThreadNum = 1;

unsigned kSelfTierId = kRoutingTierId;

vector<unsigned> kSelfTierIdVector = {kSelfTierId};
map<TierId, TierMetadata> kTierMetadata = {};

unsigned kEbsThreadNum = 1;
unsigned kMemoryThreadNum = 1;
unsigned kRoutingThreadCount = 1;

int main(int argc, char* argv[]) {
  logger->set_level(spdlog::level::off);
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
