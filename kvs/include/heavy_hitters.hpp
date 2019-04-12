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
#include <cmath>
#include <cstdlib>
#include <ctime>
#include <iostream>
#include <unordered_map>
#include <unordered_set>
#include <vector>

typedef std::string Key;

const long large_prime = 4294967311l;

class HeavyHittersSketch {
 protected:
  unsigned l, B;

  int **hash_functions;
  int **sketch_array;

  unsigned long hash_key(Key key) {
    unsigned long hash = 5381;
    int char_int = 0;
    for (unsigned i = 0; i < key.length(); i++) {
      char_int = (int)(key.at(i));
      hash = ((hash << 5) + hash) + char_int;
    }

    return hash;
  };

  unsigned location(int i, unsigned long hash) {
    return (
        unsigned)(((long)hash_functions[i][0] * hash + hash_functions[i][1]) %
                  large_prime % B);
  };

  void set_values(int **hash_functions_arg, int l_arg, int B_arg) {
    hash_functions = hash_functions_arg;
    l = l_arg;
    B = B_arg;

    sketch_array = new int *[l];
    for (unsigned i = 0; i < l; i++) {
      sketch_array[i] = new int[B];
      for (unsigned j = 0; j < B; j++) {
        sketch_array[i][j] = 0;
      }
    }
  };

 public:
  HeavyHittersSketch(int **hash_functions_arg, int l_arg, int B_arg) {
    set_values(hash_functions_arg, l_arg, B_arg);
  };

  int update(Key key) {
    unsigned long hash = hash_key(key);

    int mincount = 0;
    unsigned hashed_location = 0;
    for (unsigned i = 0; i < l; i++) {
      hashed_location = location(i, hash);
      sketch_array[i][hashed_location] = sketch_array[i][hashed_location] + 1;
      if ((sketch_array[i][hashed_location] < mincount) or (i == 0)) {
        mincount = sketch_array[i][hashed_location];
      }
    }

    return mincount;
  };

  int estimate(Key key) {
    unsigned long hash = hash_key(key);

    int mincount = 0;
    unsigned hashed_location = 0;
    for (unsigned i = 0; i < l; i++) {
      hashed_location = location(i, hash);
      if ((sketch_array[i][hashed_location] < mincount) or (i == 0)) {
        mincount = sketch_array[i][hashed_location];
      }
    }

    return mincount;
  };

  void reset(int **hash_functions_arg, int l_arg, int B_arg) {
    set_values(hash_functions_arg, l_arg, B_arg);
  };
};