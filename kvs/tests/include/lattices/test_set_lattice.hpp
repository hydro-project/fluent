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

class SetLatticeTest : public ::testing::Test {
 protected:
  SetLattice<char>* sl;
  std::unordered_set<char> set1 = {'a', 'b', 'c'};
  std::unordered_set<char> set2 = {'c', 'd', 'e'};
  std::unordered_set<char> set3 = {'a', 'd', 'e', 'b', 'c'};
  SetLatticeTest() { sl = new SetLattice<char>; }
  virtual ~SetLatticeTest() { delete sl; }
};

const int flow_test_set() { return 5; }

TEST_F(SetLatticeTest, Assign) {
  EXPECT_EQ(0, sl->size().reveal());
  sl->assign(set1);
  EXPECT_EQ(3, sl->size().reveal());
  EXPECT_EQ(set1, sl->reveal());
}

TEST_F(SetLatticeTest, MergeByValue) {
  EXPECT_EQ(0, sl->size().reveal());
  sl->merge(set1);
  EXPECT_EQ(3, sl->size().reveal());
  EXPECT_EQ(set1, sl->reveal());
  sl->merge(set2);
  EXPECT_EQ(5, sl->size().reveal());
  EXPECT_EQ(set3, sl->reveal());
}

TEST_F(SetLatticeTest, MergeByLattice) {
  EXPECT_EQ(0, sl->size().reveal());
  sl->merge(SetLattice<char>(set1));
  EXPECT_EQ(3, sl->size().reveal());
  EXPECT_EQ(set1, sl->reveal());
  sl->merge(SetLattice<char>(set2));
  EXPECT_EQ(5, sl->size().reveal());
  EXPECT_EQ(set3, sl->reveal());
}

TEST_F(SetLatticeTest, Intersection) {
  sl->merge(set1);
  SetLattice<char> res = sl->intersect(set2);
  EXPECT_EQ(std::unordered_set<char>({'c'}), res.reveal());
}
