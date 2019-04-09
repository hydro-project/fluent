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

#include "causal_cache_utils.hpp"

TEST_F(CausalCacheTest, VectorClockComparison) {
  VectorClock vc1;
  vc1.insert("c1", 1);
  vc1.insert("c2", 0);

  VectorClock vc2;
  vc2.insert("c1", 0);

  unsigned result = vector_clock_comparison(vc2, vc1);
  EXPECT_EQ(result, kCausalLess);

  vc2.insert("c2", 1);

  result = vector_clock_comparison(vc2, vc1);
  EXPECT_EQ(result, kCausalConcurrent);

  vc2.insert("c1", 1);

  result = vector_clock_comparison(vc2, vc1);
  EXPECT_EQ(result, kCausalGreaterOrEqual);

  vc1.insert("c2", 1);

  result = vector_clock_comparison(vc2, vc1);
  EXPECT_EQ(result, kCausalGreaterOrEqual);
}

TEST_F(CausalCacheTest, CausalLatticeComparison) {
  CrossCausalPayload<SetLattice<string>> ccp1;
  ccp1.vector_clock.insert("c1", 1);
  ccp1.vector_clock.insert("c2", 0);
  std::shared_ptr<CrossCausalLattice<SetLattice<string>>> ccl1 =
      std::make_shared<CrossCausalLattice<SetLattice<string>>>(ccp1);

  CrossCausalPayload<SetLattice<string>> ccp2;
  ccp2.vector_clock.insert("c1", 0);
  ccp2.vector_clock.insert("c2", 1);
  std::shared_ptr<CrossCausalLattice<SetLattice<string>>> ccl2 =
      std::make_shared<CrossCausalLattice<SetLattice<string>>>(ccp2);

  unsigned result = causal_comparison(ccl1, ccl2);
  EXPECT_EQ(result, kCausalConcurrent);
}

TEST_F(CausalCacheTest, CausalMerge) {
  CrossCausalPayload<SetLattice<string>> ccp1;
  ccp1.vector_clock.insert("c1", 1);
  ccp1.vector_clock.insert("c2", 0);
  std::shared_ptr<CrossCausalLattice<SetLattice<string>>> ccl1 =
      std::make_shared<CrossCausalLattice<SetLattice<string>>>(ccp1);
  EXPECT_EQ(ccl1.use_count(), 1);

  CrossCausalPayload<SetLattice<string>> ccp2;
  ccp2.vector_clock.insert("c1", 0);
  ccp2.vector_clock.insert("c2", 1);
  std::shared_ptr<CrossCausalLattice<SetLattice<string>>> ccl2 =
      std::make_shared<CrossCausalLattice<SetLattice<string>>>(ccp2);
  EXPECT_EQ(ccl2.use_count(), 1);

  auto ccl3 = causal_merge(ccl1, ccl2);
  map<string, int> expected = {{"c1", 1}, {"c2", 1}};
  map<string, int> result;

  for (const auto& pair : ccl3->reveal().vector_clock.reveal()) {
    result.insert(std::make_pair(pair.first, pair.second.reveal()));
  }
  EXPECT_THAT(result, testing::UnorderedElementsAreArray(expected));
  EXPECT_EQ(ccl1.use_count(), 1);
  EXPECT_EQ(ccl2.use_count(), 1);
  EXPECT_EQ(ccl3.use_count(), 1);

  auto ccl4 = causal_merge(ccl1, ccl3);
  EXPECT_THAT(result, testing::UnorderedElementsAreArray(expected));
  EXPECT_EQ(ccl1.use_count(), 1);
  EXPECT_EQ(ccl2.use_count(), 1);
  EXPECT_EQ(ccl3.use_count(), 2);
  EXPECT_EQ(ccl4.use_count(), 2);
}