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

#include "kvs_async_client.hpp"
#include "yaml-cpp/yaml.h"

#include <assert.h>

unsigned kRoutingThreadCount;

ZmqUtil zmq_util;
ZmqUtilInterface* kZmqUtil = &zmq_util;

void handle_request(KvsAsyncClientInterface* client, string input) {
  vector<string> v;
  split(input, ' ', v);

  if (v[0] == "GET") {
    client->get_async(v[1]);

    vector<KeyResponse> responses = client->receive_async(kZmqUtil);
    while (responses.size() == 0) {
      responses = client->receive_async(kZmqUtil);
    }

    if (responses.size() > 1) {
      std::cout << "Error: received more than one response" << std::endl;
    }

    assert(responses[0].tuples(0).lattice_type() == LatticeType::LWW);

    LWWPairLattice<string> lww_lattice =
        deserialize_lww(responses[0].tuples(0).payload());
    std::cout << lww_lattice.reveal().value << std::endl;
  } else if (v[0] == "GET_CAUSAL") {
    // currently this mode is only for testing purpose
    client->get_async(v[1]);

    vector<KeyResponse> responses = client->receive_async(kZmqUtil);
    while (responses.size() == 0) {
      responses = client->receive_async(kZmqUtil);
    }

    if (responses.size() > 1) {
      std::cout << "Error: received more than one response" << std::endl;
    }

    assert(responses[0].tuples(0).lattice_type() == LatticeType::CROSSCAUSAL);

    CrossCausalLattice<SetLattice<string>> ccl =
        CrossCausalLattice<SetLattice<string>>(to_cross_causal_payload(
            deserialize_cross_causal(responses[0].tuples(0).payload())));

    for (const auto& pair : ccl.reveal().vector_clock.reveal()) {
      std::cout << "{" << pair.first << " : "
                << std::to_string(pair.second.reveal()) << "}" << std::endl;
    }

    for (const auto& dep_key_vc_pair : ccl.reveal().dependency.reveal()) {
      std::cout << dep_key_vc_pair.first << " : ";
      for (const auto& vc_pair : dep_key_vc_pair.second.reveal()) {
        std::cout << "{" << vc_pair.first << " : "
                  << std::to_string(vc_pair.second.reveal()) << "}"
                  << std::endl;
      }
    }

    std::cout << *(ccl.reveal().value.reveal().begin()) << std::endl;
  } else if (v[0] == "PUT") {
    Key key = v[1];
    LWWPairLattice<string> val(
        TimestampValuePair<string>(generate_timestamp(0), v[2]));

    string req_id = client->put_async(key, serialize(val), LatticeType::LWW);

    vector<KeyResponse> responses = client->receive_async(kZmqUtil);
    while (responses.size() == 0) {
      responses = client->receive_async(kZmqUtil);
    }

    if (responses.size() > 1) {
      std::cout << "Error: received more than one response" << std::endl;
    }

    if (responses[0].response_id() != req_id) {
      std::cout << "Error: request response ID mismatch" << std::endl;
    } else {
      std::cout << "Success!" << std::endl;
    }
  } else if (v[0] == "PUT_CAUSAL") {
    // currently this mode is only for testing purpose
    Key key = v[1];

    CrossCausalPayload<SetLattice<string>> ccp;
    // construct a test client id - version pair
    ccp.vector_clock.insert("test", 1);

    // construct one test dependency
    ccp.dependency.insert(
        "dep1", VectorClock(map<string, MaxLattice<unsigned>>({{"test1", 1}})));

    // populate the value
    ccp.value.insert(v[2]);

    CrossCausalLattice<SetLattice<string>> ccl(ccp);

    string req_id =
        client->put_async(key, serialize(ccl), LatticeType::CROSSCAUSAL);

    vector<KeyResponse> responses = client->receive_async(kZmqUtil);
    while (responses.size() == 0) {
      responses = client->receive_async(kZmqUtil);
    }

    if (responses.size() > 1) {
      std::cout << "Error: received more than one response" << std::endl;
    }

    if (responses[0].response_id() != req_id) {
      std::cout << "Error: request response ID mismatch" << std::endl;
    } else {
      std::cout << "Success!" << std::endl;
    }
  } else {
    std::cout << "Unrecognized command " << v[0]
              << ". Valid commands are GET and PUT.";
  }
}

void run(KvsAsyncClientInterface* client) {
  string input;
  while (true) {
    std::cout << "kvs> ";

    getline(std::cin, input);
    handle_request(client, input);
  }
}

void run(KvsAsyncClientInterface* client, string filename) {
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

  YAML::Node user = conf["user"];
  Address ip = user["ip"].as<Address>();

  vector<Address> routing_ips;
  if (YAML::Node elb = user["routing-elb"]) {
    routing_ips.push_back(elb.as<string>());
  } else {
    YAML::Node routing = user["routing"];
    for (const YAML::Node& node : routing) {
      routing_ips.push_back(node.as<Address>());
    }
  }

  vector<UserRoutingThread> threads;
  for (Address addr : routing_ips) {
    for (unsigned i = 0; i < kRoutingThreadCount; i++) {
      threads.push_back(UserRoutingThread(addr, i));
    }
  }

  KvsAsyncClient cl(threads, ip, 0, 10000);
  KvsAsyncClientInterface* client = &cl;

  if (argc == 2) {
    run(client);
  } else {
    run(client, argv[2]);
  }
}
