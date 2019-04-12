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

#include "gmock/gmock.h"
#include "gtest/gtest.h"

#include "causal_cache_base.hpp"
#include "test_causal_lattice_comparison.hpp"
#include "test_cross_obj_causal.hpp"
#include "test_single_obj_causal.hpp"

int main(int argc, char* argv[]) {
  log_->set_level(spdlog::level::info);
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
