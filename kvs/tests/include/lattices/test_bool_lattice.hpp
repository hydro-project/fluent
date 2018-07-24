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

class BoolLatticeTest : public ::testing::Test {
 protected:
  BoolLattice* bl;
  BoolLatticeTest() { bl = new BoolLattice; }
  virtual ~BoolLatticeTest() { delete bl; }
};

const int foo() { return 5; }

TEST_F(BoolLatticeTest, Assign) {
  EXPECT_EQ(false, bl->reveal());
  bl->assign(true);
  EXPECT_EQ(true, bl->reveal());
  bl->assign(false);
  EXPECT_EQ(false, bl->reveal());
}

TEST_F(BoolLatticeTest, MergeByValue) {
  EXPECT_EQ(false, bl->reveal());
  bl->merge(true);
  EXPECT_EQ(true, bl->reveal());
  bl->merge(false);
  EXPECT_EQ(true, bl->reveal());
}

TEST_F(BoolLatticeTest, MergeByLattice) {
  EXPECT_EQ(false, bl->reveal());
  bl->merge(BoolLattice(true));
  EXPECT_EQ(true, bl->reveal());
  bl->merge(BoolLattice(false));
  EXPECT_EQ(true, bl->reveal());
}
