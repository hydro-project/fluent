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

#include <fstream>

#include "client.hpp"
#include "yaml-cpp/yaml.h"

unsigned kRoutingThreadCount;
unsigned kDefaultLocalReplication;

ZmqUtil zmq_util;
ZmqUtilInterface* kZmqUtil = &zmq_util;

void print_set(set<string> set) {
  std::cout << "{ ";
  for (const string& val : set) {
    std::cout << val << " ";
  }

  std::cout << "}" << std::endl;
}

void handle_request(KvsClient& client, string input) {
  vector<string> v;
  split(input, ' ', v);

  if (v[0] == "GET") {
    std::cout << client.get(v[1]) << std::endl;
  } else if (v[0] == "GET_SET") {
    print_set(client.get_set(v[1]));
  } else if (v[0] == "PUT") {
    if (client.put(v[1], v[2])) {
      std::cout << "Success!" << std::endl;
    } else {
      std::cout << "Failure!" << std::endl;
    }
  } else if (v[0] == "PUT_SET") {
    set<string> set;
    for (int i = 2; i < v.size(); i++) {
      set.insert(v[i]);
    }

    if (client.put(v[1], set)) {
      std::cout << "Success!" << std::endl;
    } else {
      std::cout << "Failure!" << std::endl;
    }
  } else if (v[0] == "GET_ALL") {
    auto responses = client.get_all(v[1]);
    for (const auto& response : responses) {
      std::cout << response << std::endl;
    }
  } else if (v[0] == "GET_SET_ALL") {
    vector<set<string>> result = client.get_set_all(v[1]);

    for (const auto& set : result) {
      print_set(set);
    }
  } else if (v[0] == "PUT_ALL") {
    if (client.put_all(v[1], v[2])) {
      std::cout << "Success!" << std::endl;
    } else {
      std::cout << "Failure!" << std::endl;
    }
  } else if (v[0] == "PUT_SET_ALL") {
    set<string> set;
    for (int i = 2; i < v.size(); i++) {
      set.insert(v[i]);
    }

    if (client.put_all(v[1], set)) {
      std::cout << "Success!" << std::endl;
    } else {
      std::cout << "Failure!" << std::endl;
    }
  } else {
    std::cout << "Unrecognized command " << v[0]
              << ". Valid commands are GET, GET_SET, GET_ALL, GET_SET_ALL, "
                 "PUT, PUT_SET, PUT_ALL, and PUT_SET_ALL.";
  }
}

void run(KvsClient& client) {
  string input;
  while (true) {
    std::cout << "kvs> ";

    getline(std::cin, input);
    handle_request(client, input);
  }
}

void run(KvsClient& client, string filename) {
  string input;
  std::ifstream infile(filename);

  while (getline(infile, input)) {
    handle_request(client, input);
  }
}

int main(int argc, char* argv[]) {
  if (argc < 2 || argc > 3) {
    std::cerr << "Usage: " << argv[0] << " conf-file <input-file>" << std::endl;
    std::cerr
        << "Filename is optional. Omit the filename to run in interactive mode."
        << std::endl;
    return 1;
  }

  // read the YAML conf
  YAML::Node conf = YAML::LoadFile(argv[1]);
  kRoutingThreadCount = conf["threads"]["routing"].as<unsigned>();
  kDefaultLocalReplication = conf["replication"]["local"].as<unsigned>();

  YAML::Node user = conf["user"];
  Address ip = user["ip"].as<Address>();

  vector<Address> routing_addresses;
  bool local;
  if (YAML::Node elb = user["routing-elb"]) {
    routing_addresses.push_back(elb.as<string>());
    local = false;
  } else {
    YAML::Node routing = user["routing"];
    local = true;

    for (const YAML::Node& node : routing) {
      routing_addresses.push_back(node.as<Address>());
    }
  }

  KvsClient client(routing_addresses, kRoutingThreadCount, ip, 0, 10000, local);
  if (argc == 2) {
    run(client);
  } else {
    run(client, argv[2]);
  }
}
