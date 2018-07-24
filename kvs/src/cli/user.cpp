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

#include "hash_ring.hpp"
#include "requests.hpp"
#include "spdlog/spdlog.h"
#include "threads.hpp"
#include "yaml-cpp/yaml.h"

unsigned kRoutingThreadCount;
unsigned kDefaultLocalReplication;

ZmqUtil zmq_util;
ZmqUtilInterface* kZmqUtil = &zmq_util;

HashRingUtil hash_ring_util;
HashRingUtilInterface* kHashRingUtil = &hash_ring_util;

void handle_request(
    std::string request_line, SocketCache& pushers,
    std::vector<Address>& routing_addresses,
    std::unordered_map<Key, std::unordered_set<Address>>& key_address_cache,
    unsigned& seed, std::shared_ptr<spdlog::logger> logger, UserThread& ut,
    zmq::socket_t& response_puller, zmq::socket_t& key_address_puller,
    Address& ip, unsigned& thread_id, unsigned& rid, unsigned& trial) {
  std::vector<std::string> v;
  split(request_line, ' ', v);
  Key key;
  std::string value;

  if (!((v.size() == 2 && v[0] == "GET") || (v.size() == 3 && v[0] == "PUT"))) {
    std::cerr << "Usage: GET <key> | PUT <key> <value>" << std::endl;
    return;
  } else {
    if (v[0] == "GET") {
      key = v[1];
      value = "";
    } else {
      key = v[1];
      value = v[2];
    }
  }

  if (trial > 5) {
    logger->info("Trial #{} for request for key {}.", trial, key);
    logger->info("Waiting 5 seconds.");
    std::chrono::seconds dura(5);
    std::this_thread::sleep_for(dura);
    logger->info("Waited 5s.");
  }

  // get worker address
  Address worker_address;
  if (key_address_cache.find(key) == key_address_cache.end()) {
    // query the routing and update the cache
    Address target_routing_address =
        kHashRingUtil
            ->get_random_routing_thread(routing_addresses, seed,
                                        kRoutingThreadCount)
            .get_key_address_connect_addr();
    bool succeed;
    std::vector<Address> addresses = kHashRingUtil->get_address_from_routing(
        ut, key, pushers[target_routing_address], key_address_puller, succeed,
        ip, thread_id, rid);

    if (succeed) {
      for (const std::string& address : addresses) {
        key_address_cache[key].insert(address);
      }
      worker_address = addresses[rand_r(&seed) % addresses.size()];
    } else {
      logger->error(
          "Request timed out when querying routing. This should never happen!");
      return;
    }
  } else {
    if (key_address_cache[key].size() == 0) {
      logger->error("Address cache for key " + key + " has size 0.");
      return;
    }

    worker_address = *(next(begin(key_address_cache[key]),
                            rand_r(&seed) % key_address_cache[key].size()));
  }

  KeyRequest req;
  req.set_response_address(ut.get_request_pulling_connect_addr());

  std::string req_id =
      ip + ":" + std::to_string(thread_id) + "_" + std::to_string(rid);
  req.set_request_id(req_id);
  rid += 1;

  KeyTuple* tp = req.add_tuples();
  tp->set_key(key);
  tp->set_address_cache_size(key_address_cache[key].size());

  if (value == "") {
    // get request
    req.set_type(get_request_type("GET"));
  } else {
    // put request
    req.set_type(get_request_type("PUT"));
    tp->set_value(value);
    tp->set_timestamp(0);
  }

  bool succeed;
  auto res = send_request<KeyRequest, KeyResponse>(req, pushers[worker_address],
                                                   response_puller, succeed);

  if (succeed) {
    KeyTuple tuple = res.tuples(0);

    // initialize the respond string
    if (tuple.error() == 2) {
      trial += 1;
      if (trial > 5) {
        for (const auto& address : res.tuples(0).addresses()) {
          logger->info("Server's return address for key {} is {}.", key,
                       address);
        }

        for (const std::string& address : key_address_cache[key]) {
          logger->info("My cached address for key {} is {}", key, address);
        }
      }

      // update cache and retry
      key_address_cache.erase(key);
      handle_request(request_line, pushers, routing_addresses,
                     key_address_cache, seed, logger, ut, response_puller,
                     key_address_puller, ip, thread_id, rid, trial);
    } else {
      // succeeded
      if (tuple.has_invalidate() && tuple.invalidate()) {
        // update cache
        key_address_cache.erase(key);
      }
      if (value == "" && tuple.error() == 0) {
        std::cout << "value of key " + tuple.key() + " is " + tuple.value() +
                         "\n";
      } else if (value == "" && tuple.error() == 1) {
        std::cout << "key " + tuple.key() + " does not exist\n";
      } else if (value != "") {
        std::cout << "successfully put key " + tuple.key() + "\n";
      }
    }
  } else {
    logger->info(
        "Request timed out when querying worker: clearing cache due to "
        "possible node membership changes.");
    // likely the node has departed. We clear the entries relavant to the
    // worker_address
    std::vector<std::string> tokens;
    split(worker_address, ':', tokens);
    std::string signature = tokens[1];
    std::unordered_set<Key> remove_set;

    for (const auto& key_pair : key_address_cache) {
      for (const std::string& address : key_pair.second) {
        std::vector<std::string> v;
        split(address, ':', v);

        if (v[1] == signature) {
          remove_set.insert(key_pair.first);
        }
      }
    }

    for (const std::string& key : remove_set) {
      key_address_cache.erase(key);
    }

    trial += 1;
    handle_request(request_line, pushers, routing_addresses, key_address_cache,
                   seed, logger, ut, response_puller, key_address_puller, ip,
                   thread_id, rid, trial);
  }
}

void run(unsigned thread_id, std::string filename, Address ip,
         std::vector<Address> routing_addresses) {
  std::string log_file = "log_user.txt";
  std::string logger_name = "user_log";
  auto logger = spdlog::basic_logger_mt(logger_name, log_file, true);
  logger->flush_on(spdlog::level::info);

  std::hash<std::string> hasher;
  unsigned seed = time(NULL);
  seed += hasher(ip);
  seed += thread_id;
  logger->info("Random seed is {}.", seed);

  // mapping from key to a set of worker addresses
  std::unordered_map<Key, std::unordered_set<Address>> key_address_cache;

  UserThread ut = UserThread(ip, thread_id);

  int timeout = 10000;
  zmq::context_t context(1);
  SocketCache pushers(&context, ZMQ_PUSH);

  // responsible for pulling response
  zmq::socket_t response_puller(context, ZMQ_PULL);
  response_puller.setsockopt(ZMQ_RCVTIMEO, &timeout, sizeof(timeout));
  response_puller.bind(ut.get_request_pulling_bind_addr());

  // responsible for receiving key address responses
  zmq::socket_t key_address_puller(context, ZMQ_PULL);
  key_address_puller.setsockopt(ZMQ_RCVTIMEO, &timeout, sizeof(timeout));
  key_address_puller.bind(ut.get_key_address_bind_addr());

  unsigned rid = 0;

  std::string input;
  unsigned trial = 1;
  if (filename == "") {
    while (true) {
      std::cout << "kvs> ";

      getline(std::cin, input);
      handle_request(input, pushers, routing_addresses, key_address_cache, seed,
                     logger, ut, response_puller, key_address_puller, ip,
                     thread_id, rid, trial);
    }
  } else {
    std::ifstream infile(filename);

    while (getline(infile, input)) {
      handle_request(input, pushers, routing_addresses, key_address_cache, seed,
                     logger, ut, response_puller, key_address_puller, ip,
                     thread_id, rid, trial);
    }
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
  YAML::Node routing = user["routing"];
  std::vector<Address> routing_addresses;

  for (const YAML::Node& node : routing) {
    routing_addresses.push_back(node.as<Address>());
  }
  if (argc == 1) {
    run(0, "", ip, routing_addresses);
  } else {
    run(0, argv[1], ip, routing_addresses);
  }
}
