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

#include "common.hpp"
#include "hash_ring.hpp"
#include "kvs_common.hpp"
#include "threads.hpp"
#include "yaml-cpp/yaml.h"

// TODO(vikram): We probably don't want to have to define all of these here?
ZmqUtil zmq_util;
ZmqUtilInterface* kZmqUtil = &zmq_util;

HashRingUtil hash_ring_util;
HashRingUtilInterface* kHashRingUtil = &hash_ring_util;

unsigned kBenchmarkThreadNum = 1;
unsigned kRoutingThreadCount = 1;
unsigned kDefaultLocalReplication = 1;

int main(int argc, char* argv[]) {
  if (argc != 2) {
    std::cerr << "Usage: " << argv[0] << " <benchmark_threads>" << std::endl;
    return 1;
  }

  unsigned thread_num = atoi(argv[1]);

  // read the YAML conf
  vector<Address> benchmark_address;
  YAML::Node conf = YAML::LoadFile("conf/kvs-config.yml");
  YAML::Node benchmark = conf["benchmark"];

  for (const YAML::Node& node : benchmark) {
    benchmark_address.push_back(node.as<Address>());
  }

  zmq::context_t context(1);
  SocketCache pushers(&context, ZMQ_PUSH);

  string command;
  while (true) {
    std::cout << "command> ";
    getline(std::cin, command);

    for (const Address address : benchmark_address) {
      for (unsigned tid = 0; tid < thread_num; tid++) {
        BenchmarkThread bt = BenchmarkThread(address, tid);

        kZmqUtil->send_string(command,
                              &pushers[bt.benchmark_command_address()]);
      }
    }
  }
}
