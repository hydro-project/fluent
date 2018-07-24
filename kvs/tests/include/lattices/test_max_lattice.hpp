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

template <typename T>
class MaxLatticeTest : public ::testing::Test {
 protected:
  MaxLattice<T>* ml;
  MaxLatticeTest() { ml = new MaxLattice<T>; }
  virtual ~MaxLatticeTest() { delete ml; }
};

typedef ::testing::Types<int, float, double> MaxTypes;
TYPED_TEST_CASE(MaxLatticeTest, MaxTypes);

TYPED_TEST(MaxLatticeTest, Assign) {
  EXPECT_EQ(0, this->ml->reveal());
  this->ml->assign(10);
  EXPECT_EQ(10, this->ml->reveal());
  this->ml->assign(5);
  EXPECT_EQ(5, this->ml->reveal());
}

TYPED_TEST(MaxLatticeTest, MergeByValue) {
  EXPECT_EQ(0, this->ml->reveal());
  this->ml->merge(10);
  EXPECT_EQ(10, this->ml->reveal());
  this->ml->merge(5);
  EXPECT_EQ(10, this->ml->reveal());
}

TYPED_TEST(MaxLatticeTest, MergeByLattice) {
  EXPECT_EQ(0, this->ml->reveal());
  this->ml->merge(MaxLattice<TypeParam>(10));
  EXPECT_EQ(10, this->ml->reveal());
  this->ml->merge(MaxLattice<TypeParam>(5));
  EXPECT_EQ(10, this->ml->reveal());
}

TYPED_TEST(MaxLatticeTest, Add) {
  MaxLattice<TypeParam> res = this->ml->add(5);
  EXPECT_EQ(5, res.reveal());
}

TYPED_TEST(MaxLatticeTest, Subtract) {
  MaxLattice<TypeParam> res = this->ml->subtract(5);
  EXPECT_EQ(-5, res.reveal());
}
