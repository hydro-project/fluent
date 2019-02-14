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
#include <unordered_set>

#include "client.hpp"
#include "yaml-cpp/yaml.h"

unsigned kRoutingThreadCount;
unsigned kDefaultLocalReplication;

ZmqUtil zmq_util;
ZmqUtilInterface* kZmqUtil = &zmq_util;

void run(KvsClient client) {
  std::string input;
  while (true) {
    std::cout << "kvs> ";

    getline(std::cin, input);
    handle_request(client, input);
  }
}

void run(KvsClient client, std::string filename) {
  std::ifstream infile(filename);

  while (getline(infile, input)) {
    handle_request(client, input);
  }
}

void handle_request(std::string input, KvsClient client) {
  std::vector<std::string> v;
  split(request_line, ' ', v);
  Key key;
  std::string value;

  if (v[0] == "GET") {
    cout << client.get(key);
  } else if (v[0] == "PUT") {
    if (client.put(v[1], v[2])) {
      cout << "Success!";
    } else {
      cout << "Failure!";
    }
  } else if (v[0] == "GET_ALL") {
    auto responses = client.get_all(key);
    for (const auto& response : responses) {
      std::cout << response;
    }
  } else if (v[0] == "PUT_ALL") {
    if (client.put_all(v[1], v[2])) {
      cout << "Success!";
    } else {
      cout << "Failure!";
    }
  } else {
    cout << "Unrecognized command " << v[0] << ". Valid commands are GET, GET_ALL, PUT, and PUT_ALL.";
  }
}

int main(int argc, char* argv[]) {
  if (argc > 2) {
    std::cerr << "Usage: " << argv[0] << "<filename>" << std::endl;
    std::cerr
        << "Filename is optional. Omit the filename to run in interactive mode."
        << std::endl;
    return 1;
  }

  // read the YAML conf
  YAML::Node conf = YAML::LoadFile("conf/kvs-config.yml");
  kRoutingThreadCount = conf["threads"]["routing"].as<unsigned>();
  kDefaultLocalReplication = conf["replication"]["local"].as<unsigned>();

  YAML::Node user = conf["user"];
  Address ip = user["ip"].as<Address>();

  KvsClient client;
  if (YAML::Node elb = user["routing-elb"]) {
    client = (elb.as<std::string>(), kRoutingThreadCount, ip, 0);
  } else {
    YAML::Node routing = user["routing"];
    std::vector<Address> routing_addresses;

    for (const YAML::Node& node : routing) {
      routing_addresses.push_back(node.as<Address>());
    }

    client = (routing_addresses, kRoutingThreadCount, ip, 0);
  }

  if (argc == 1) {
    run(client);
  } else {
    run(client, argv[1]);
  }
}
