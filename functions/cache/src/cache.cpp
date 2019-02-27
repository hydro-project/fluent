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

#include "kvs_client.hpp"
#include "yaml-cpp/yaml.h"

ZmqUtil zmq_util;
ZmqUtilInterface* kZmqUtil = &zmq_util;

void run(KvsClient& client, Address ip, unsigned thread_id) {
  string log_file = "log_" + std::to_string(thread_id) + ".txt";
  string log_name = "cache_log_" + std::to_string(thread_id);
  auto log = spdlog::basic_logger_mt(log_name, log_file, true);
  log->flush_on(spdlog::level::info);

  unsigned seed = time(NULL);

  zmq::context_t context(1);
  SocketCache pushers(&context, ZMQ_PUSH);

  map<Key, LWWPairLattice<string>> local_lww_cache;
  map<Key, SetLattice<string>> local_set_cache;

  map<Key, LatticeType> key_type_map;

  CacheThread ct = CacheThread(ip, thread_id);

  // TODO: can we find a way to make the thread classes uniform across
  // languages? or unify the python and cpp implementations; actually, mostly
  // just the user thread stuff, I think.
  zmq::socket_t get_responder(context, ZMQ_REP);
  get_responder.bind(ct.cache_get_bind_address());

  zmq::socket_t put_responder(context, ZMQ_REP);
  put_responder.bind(ct.cache_put_bind_address());

  // TODO: bind update address here

  vector<zmq::pollitem_t> pollitems = {
      {static_cast<void*>(get_responder), 0, ZMQ_POLLIN, 0},
      {static_cast<void*>(put_responder), 0, ZMQ_POLLIN, 0},
  };

  while (true) {
    kZmqUtil->poll(0, &pollitems);

    // handle a GET request
    if (pollitems[0].revents & ZMQ_POLLIN) {
      string serialized = kZmqUtil->recv_string(&get_responder);
      KeyRequest request;
      request.ParseFromString(serialized);

      KeyResponse response;

      for (KeyTuple tuple : request.tuples()) {
        KeyTuple* resp = response.add_tuples();
        Key key = tuple.key();
        resp->set_key(key);

        if (!tuple.has_lattice_type()) {
          log->error("Cache requires type to retrieve key.");
          resp->set_error(3);
          continue;
        }

        switch (tuple.lattice_type()) {
          case LatticeType::LWW: {
            if (local_lww_cache.find(key) == local_lww_cache.end()) {
              LWWPairLattice<string> resp = client.get(key);
              local_lww_cache[key] = resp;
              key_type_map[key] = LatticeType::LWW;
            }

            resp->set_payload(serialize(local_lww_cache[key]));
            resp->set_lattice_type(LatticeType::LWW);
            break;
          }
          case LatticeType::SET: {
            if (local_set_cache.find(key) == local_set_cache.end()) {
              SetLattice<string> resp = client.get_set(key);
              local_set_cache[key] = resp;
              key_type_map[key] = LatticeType::SET;
            }

            resp->set_payload(serialize(local_set_cache[key]));
            resp->set_lattice_type(LatticeType::SET);
            break;
          }
          default: {
            resp->set_error(3);
          }
        }
      }

      std::string resp_string;
      response.SerializeToString(&resp_string);
      kZmqUtil->send_string(resp_string, &get_responder);
    }

    // handle a PUT request
    if (pollitems[1].revents & ZMQ_POLLIN) {
      string serialized = kZmqUtil->recv_string(&get_responder);
      KeyRequest request;
      request.ParseFromString(serialized);

      KeyResponse response;

      for (KeyTuple tuple : request.tuples()) {
        KeyTuple* resp = response.add_tuples();
        Key key = tuple.key();
        resp->set_key(key);

        LatticeType type = tuple.lattice_type();

        if (key_type_map.find(key) != key_type_map.end()) {
          if (key_type_map[key] != type) {
            resp->set_error(2);
            continue;
          }
        }

        switch (type) {
          case LatticeType::LWW: {
            LWWPairLattice<string> new_val = deserialize_lww(tuple.payload());
            if (local_lww_cache.find(key) != local_lww_cache.end()) {
              new_val.merge(local_lww_cache[key]);
            }

            local_lww_cache[key] = new_val;
            resp->set_error(0);
          }
          case LatticeType::SET: {
            SetLattice<string> new_val = deserialize_set(tuple.payload());
            if (local_set_cache.find(key) != local_set_cache.end()) {
              new_val.merge(local_set_cache[key]);
            }

            local_set_cache[key] = new_val;
            resp->set_error(0);
          }
          default: resp->set_error(2);
        }
      }

      std::string resp_string;
      response.SerializeToString(&resp_string);
      kZmqUtil->send_string(resp_string, &get_responder);
    }

    // TODO: check & update different caches
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

  KvsClient client(threads, ip, 0, 10000);

  run(client, ip, 0);
}
