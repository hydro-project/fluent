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

#include "gtest/gtest.h"
#include "lattices/core_lattices.hpp"

typedef std::unordered_map<char, MaxLattice<int>> charMaxIntMap;

class MapLatticeTest : public ::testing::Test {
 protected:
  MapLattice<char, MaxLattice<int>>* mapl;
  charMaxIntMap map1 = {{'a', MaxLattice<int>(10)}, {'b', MaxLattice<int>(20)}};
  charMaxIntMap map2 = {{'b', MaxLattice<int>(30)}, {'c', MaxLattice<int>(40)}};
  charMaxIntMap map3 = {{'a', MaxLattice<int>(10)},
                        {'b', MaxLattice<int>(30)},
                        {'c', MaxLattice<int>(40)}};
  MapLatticeTest() { mapl = new MapLattice<char, MaxLattice<int>>; }
  virtual ~MapLatticeTest() { delete mapl; }
  void check_equality(charMaxIntMap m) {
    EXPECT_EQ(m.size(), mapl->size().reveal());
    charMaxIntMap result = mapl->reveal();
    for (auto it = result.begin(); it != result.end(); ++it) {
      ASSERT_FALSE(m.find(it->first) == m.end());
      ASSERT_TRUE(m.find(it->first)->second == it->second);
    }
  }
};

TEST_F(MapLatticeTest, Assign) {
  EXPECT_EQ(0, mapl->size().reveal());
  mapl->assign(map1);
  check_equality(map1);
}

TEST_F(MapLatticeTest, MergeByValue) {
  EXPECT_EQ(0, mapl->size().reveal());
  mapl->merge(map1);
  check_equality(map1);
  mapl->merge(map2);
  check_equality(map3);
}

TEST_F(MapLatticeTest, MergeByLattice) {
  EXPECT_EQ(0, mapl->size().reveal());
  mapl->merge(MapLattice<char, MaxLattice<int>>(map1));
  check_equality(map1);
  mapl->merge(MapLattice<char, MaxLattice<int>>(map2));
  check_equality(map3);
}

TEST_F(MapLatticeTest, KeySet) {
  mapl->merge(map1);
  SetLattice<char> res = mapl->key_set();
  EXPECT_EQ(std::unordered_set<char>({'a', 'b'}), res.reveal());
}

TEST_F(MapLatticeTest, At) {
  mapl->merge(map1);
  MaxLattice<int> res = mapl->at('a');
  EXPECT_EQ(10, res.reveal());
}

TEST_F(MapLatticeTest, Contains) {
  mapl->merge(map1);
  BoolLattice res = mapl->contains('a');
  EXPECT_EQ(true, res.reveal());
  res = mapl->contains('d');
  EXPECT_EQ(false, res.reveal());
}
