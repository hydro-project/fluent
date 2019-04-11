#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <chrono>
#include <climits>
#include <cmath>
#include <cstdlib>
#include <ctime>
#include <iomanip>
#include <iostream>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>
#include "heavy_hitters.hpp"

#define alpha 0.2

typedef std::string Key;

class AdaptiveThresholdHeavyHitters {
 protected:
  HeavyHittersSketch* hh_sketch;
  float threshold_percent = 0.01;
  float gamma = 4127;
  float epsilon = 0.001;

  std::unordered_set<Key> total_set;

  std::unordered_map<Key, int> hot_map;
  std::unordered_map<Key, int> cold_map;

  int hot_threshold;
  int cold_threshold;

  std::chrono::system_clock::time_point last_update_time;

  void set_values() {
    int B_arg = (int)(ceil(exp(1) / epsilon));
    int l_arg = (int)(ceil(log(gamma)));
    int** hash_functions_arg = get_hash_functions(l_arg);
    std::unordered_set<Key> reset_total_set;
    std::unordered_map<Key, int> reset_hot_map;
    std::unordered_map<Key, int> reset_cold_map;

    total_set = reset_total_set;
    hot_map = reset_hot_map;
    cold_map = reset_cold_map;

    hh_sketch = new HeavyHittersSketch(hash_functions_arg, l_arg, B_arg);

    hot_threshold = 0;
    cold_threshold = INT_MIN;

    last_update_time = std::chrono::system_clock::now();
  };

  int partition(int list[], int left, int right, int pivotIndex) {
    int pivotValue = list[pivotIndex];

    int tmp = list[pivotIndex];
    list[pivotIndex] = list[right];
    list[right] = tmp;

    int storeIndex = left;
    for (int i = left; i < right - 1; i++) {
      if (list[i] < pivotValue) {
        tmp = list[storeIndex];
        list[storeIndex] = list[i];
        list[i] = list[storeIndex];

        storeIndex++;
      }
    }

    tmp = list[right];
    list[right] = list[storeIndex];
    list[storeIndex] = list[right];
    return storeIndex;
  };

  int select(int list[], int left, int right, int k) {
    if (left == right) {
      return list[left];
    }

    int pivotIndex = right;

    pivotIndex = partition(list, left, right, pivotIndex);

    if (k == pivotIndex) {
      return list[k];
    } else if (k < pivotIndex) {
      return select(list, left, pivotIndex - 1, k);
    } else {
      return select(list, pivotIndex + 1, right, k);
    }
  };

  void update_hot(void) {
    int* vals;
    std::unordered_map<Key, int> new_hot_map;
    std::vector<int> val_vec;
    int val_size = 0;

    for (auto kv : hot_map) {
      val_size = val_size + 1;
      val_vec.push_back(kv.second);
    }

    vals = (int*)(&val_vec[0]);

    int median = select(vals, 0, val_size - 1, 1);

    for (auto kv : hot_map) {
      if (kv.second > median) {
        new_hot_map[kv.first] = kv.second;
      }
    }

    hot_map = new_hot_map;
    hot_threshold = median;
  };

  void update_cold(void) {
    int* vals;
    std::unordered_map<Key, int> new_cold_map;
    std::vector<int> val_vec;
    int val_size = 0;

    for (auto kv : cold_map) {
      val_size = val_size + 1;
      val_vec.push_back(kv.second);
    }

    vals = &val_vec[0];

    int median = (-1 * select(vals, 0, val_size - 1, 1));

    for (auto kv : cold_map) {
      if ((-1 * kv.second) >= median) {
        new_cold_map[kv.first] = kv.second;
      }
    }

    cold_map = new_cold_map;
    cold_threshold = median;
  };

  int** get_hash_functions(int l) {
    int** hash_functions;
    srand(time(NULL));
    hash_functions = new int*[l];
    for (unsigned i = 0; i < l; i++) {
      hash_functions[i] = new int[2];
      hash_functions[i][0] =
          int(float(rand()) * float(large_prime) / float(RAND_MAX) + 1);
      hash_functions[i][1] =
          int(float(rand()) * float(large_prime) / float(RAND_MAX) + 1);
    }
    return hash_functions;
  };

 public:
  AdaptiveThresholdHeavyHitters() { set_values(); };

  void report_key(Key key) {
    total_set.insert(key);

    int new_count = (*hh_sketch).update(key);

    if (new_count > hot_threshold) {
      hot_map[key] = new_count;
    }

    if ((-1 * new_count) >= cold_threshold) {
      cold_map[key] = new_count;
    } else {
      if (cold_map.find(key) != cold_map.end()) {
        cold_map[key] = new_count;
      }
    }

    std::chrono::system_clock::time_point now =
        std::chrono::system_clock::now();
    std::chrono::duration<double, std::milli> passed = now - last_update_time;
    double passed_num = passed.count();

    if (passed_num > 10) {
      int total_size = total_set.size();
      int hot_size = hot_map.size();
      if (hot_size > (threshold_percent * total_size)) {
        update_hot();
      }

      int cold_size = cold_map.size();
      if (cold_size > (threshold_percent * total_size)) {
        update_cold();
      }
      last_update_time = now;
    }
  };

  int get_key_count(Key key) { return (*hh_sketch).estimate(key); };

  std::unordered_map<Key, int> get_hot_map(void) { return hot_map; };

  std::unordered_map<Key, int> get_cold_map(void) { return cold_map; };

  int get_hot_threshold() { return hot_threshold; };

  int get_cold_threshold() { return cold_threshold; };

  int get_total_size() { return total_set.size(); };

  void reset() { set_values(); };

  // static void reset_threshold_percent(float new_threshold) {
  //     threshold_percent = new_threshold;
  // };

  // static void reset_error(float new_epsilon) {
  //     epsilon = new_epsilon;
  // };

  // static void update_gamma(int total_hits, int num_hh) {
  //     float avg_hit = (1.0 * total_hits) / num_hh;
  //     gamma = (alpha * avg_hit) + ((1 - alpha) * gamma);
  // };
};
