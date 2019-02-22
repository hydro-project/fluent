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

#include "client.hpp"
#include "yaml-cpp/yaml.h"

ZmqUtil zmq_util;
ZmqUtilInterface* kZmqUtil = &zmq_util;

const string request_address = "tcp://*:9000";

void run(KvsClient& client) {
  unsigned thread_id = 0;

  string log_file = "log_" + std::to_string(thread_id) + ".txt";
  string log_name = "cache_log_" + std::to_string(thread_id);
  auto log = spdlog::basic_logger_mt(log_name, log_file, true);
  log->flush_on(spdlog::level::info);

  unsigned seed = time(NULL);

  zmq::context_t context(1);
  SocketCache pushers(&context, ZMQ_PUSH);

  map<Key, std::string> local_cache;

  // TODO: create CacheThread class
  // TODO: can we find a way to make the thread classes uniform across
  // languages? or unify the python and cpp implementations; actually, mostly
  // just the user thread stuff, I think.
  // TODO: move the lattice and common and client libraries to the global
  // include -- they aren't KVS only

  zmq::socket_t get_responder(context, ZMQ_REP);
  get_responder.bind(request_address);

  zmq::socket_t put_responder(context, ZMQ_REP);
  put_responder.bind(request_address);

  vector<zmq::pollitem_t> pollitems = {
      {static_cast<void*>(get_responder), 0, ZMQ_POLLIN, 0},
      {static_cast<void*>(put_responder), 0, ZMQ_POLLIN, 0},
  };

  while (true) {
    kZmqUtil->poll(0, &pollitems);

    if (pollitems[0].revents && ZMQ_POLLIN) {
      Key key = kZmqUtil->recv_string(&get_responder);

      if (local_cache.find(key) == local_cache.end()) {
        string val = client.get(key);
        local_cache[key] = val;
      }

      kZmqUtil->send_string(local_cache[key], &get_responder);
    }

    if (pollitems[1].revents && ZMQ_POLLIN) {
    }
  }
}

int main(int argc, char* argv[]) {
  if (argc > 1) {
    std::cerr << "Usage: " << argv[0] << "" << std::endl;
    return 1;
  }

  // read the YAML conf
  YAML::Node conf = YAML::LoadFile("kvs-config.yml");
  unsigned kRoutingThreadCount = conf["threads"]["routing"].as<unsigned>();

  YAML::Node user = conf["user"];
  Address ip = user["ip"].as<Address>();

  vector<Address> routing_ips;
  bool local;
  if (YAML::Node elb = user["routing-elb"]) {
    routing_ips.push_back(elb.as<string>());
    local = false;
  } else {
    YAML::Node routing = user["routing"];
    local = true;

    for (const YAML::Node& node : routing) {
      routing_ips.push_back(node.as<Address>());
    }
  }

  KvsClient client(routing_ips, kRoutingThreadCount, ip, 0, 10000, local);

  run(client);
}
