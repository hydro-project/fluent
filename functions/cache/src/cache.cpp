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

unsigned kCacheReportThreshold = 5;

void run(KvsClient& client, Address ip, unsigned thread_id) {
  string log_file = "cache_log_" + std::to_string(thread_id) + ".txt";
  string log_name = "cache_log_" + std::to_string(thread_id);
  auto log = spdlog::basic_logger_mt(log_name, log_file, true);
  log->flush_on(spdlog::level::info);

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

  zmq::socket_t update_puller(context, ZMQ_PULL);
  update_puller.bind(ct.cache_update_bind_address());

  vector<zmq::pollitem_t> pollitems = {
      {static_cast<void*>(get_responder), 0, ZMQ_POLLIN, 0},
      {static_cast<void*>(put_responder), 0, ZMQ_POLLIN, 0},
      {static_cast<void*>(update_puller), 0, ZMQ_POLLIN, 0},
  };

  auto report_start = std::chrono::system_clock::now();
  auto report_end = std::chrono::system_clock::now();

  while (true) {
    kZmqUtil->poll(0, &pollitems);

    // handle a GET request
    if (pollitems[0].revents & ZMQ_POLLIN) {
      string serialized = kZmqUtil->recv_string(&get_responder);
      KeyRequest request;
      request.ParseFromString(serialized);

      KeyResponse response;

      response.set_type(request.type());

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
            break;
          }
        }
      }

      std::string resp_string;
      response.SerializeToString(&resp_string);
      kZmqUtil->send_string(resp_string, &get_responder);
    }

    // handle a PUT request
    if (pollitems[1].revents & ZMQ_POLLIN) {
      string serialized = kZmqUtil->recv_string(&put_responder);
      KeyRequest request;
      request.ParseFromString(serialized);

      KeyResponse response;

      response.set_type(request.type());

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
            break;
          }
          case LatticeType::SET: {
            SetLattice<string> new_val = deserialize_set(tuple.payload());
            if (local_set_cache.find(key) != local_set_cache.end()) {
              new_val.merge(local_set_cache[key]);
            }

            local_set_cache[key] = new_val;
            resp->set_error(0);
            break;
          }
          default: resp->set_error(2); break;
        }
      }

      std::string resp_string;
      response.SerializeToString(&resp_string);
      kZmqUtil->send_string(resp_string, &put_responder);

      // PUT the values into the KVS
      for (KeyTuple tuple : request.tuples()) {
        Key key = tuple.key();
        LatticeType type = tuple.lattice_type();

        switch (type) {
          case LatticeType::LWW: client.put(key, local_lww_cache[key]); break;
          case LatticeType::SET: client.put(key, local_set_cache[key]); break;
          default:  // this should never happen
            break;
        }
      }
    }

    // handle updates received from the KVS
    if (pollitems[2].revents & ZMQ_POLLIN) {
      string serialized = kZmqUtil->recv_string(&update_puller);
      KeyRequest updates;
      updates.ParseFromString(serialized);

      for (const KeyTuple& tuple : updates.tuples()) {
        Key key = tuple.key();

        // if we are no longer caching this key, then we simply ignore updates
        // for it because we received the update based on outdated information
        if (key_type_map.find(key) == key_type_map.end()) {
          continue;
        }

        if (key_type_map[key] != tuple.lattice_type()) {
          // This is bad! This means that we have a certain lattice type stored
          // locally for a key and that is incompatible with the lattice type
          // stored in the KVS. This probably means that something was put here
          // but hasn't been propagated yet, and something *different* was put
          // globally. I think we should just drop our local copy for the time
          // being, but open to other ideas.

          switch (key_type_map[key]) {
            case LatticeType::LWW: local_lww_cache.erase(key); break;
            case LatticeType::SET: local_set_cache.erase(key); break;
            default:  // this can never happen
              break;
          }

          key_type_map[key] = tuple.lattice_type();
        }

        switch (key_type_map[key]) {
          case LatticeType::LWW: {
            LWWPairLattice<string> new_val = deserialize_lww(tuple.payload());

            if (local_lww_cache.find(key) != local_lww_cache.end()) {
              new_val.merge(local_lww_cache[key]);
            }

            local_lww_cache[key] = new_val;
            break;
          }
          case LatticeType::SET: {
            SetLattice<string> new_val = deserialize_set(tuple.payload());
            if (local_set_cache.find(key) != local_set_cache.end()) {
              new_val.merge(local_set_cache[key]);
            }

            local_set_cache[key] = new_val;
          }
          default:  // this should never happen!
            break;
        }
      }
    }

    // collect and store internal statistics
    report_end = std::chrono::system_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::seconds>(
                        report_end - report_start)
                        .count();

    // update KVS with information about which keys this node is currently
    // caching; we only do this periodically because we are okay with receiving
    // potentially stale updates
    if (duration >= kCacheReportThreshold) {
      KeySet set;

      for (const auto& pair : key_type_map) {
        set.add_keys(pair.first);
      }

      string serialized;
      set.SerializeToString(&serialized);

      LWWPairLattice<string> val(TimestampValuePair<string>(
          generate_timestamp(thread_id), serialized));
      Key key = get_user_metadata_key(ip, UserMetadataType::cache_ip);
      client.put(key, val);
      report_start = std::chrono::system_clock::now();
    }

    // TODO: check if cache size is exceeding (threshold x capacity) and evict.
  }
}

int main(int argc, char* argv[]) {
  if (argc > 1) {
    std::cerr << "Usage: " << argv[0] << "" << std::endl;
    return 1;
  }

  // read the YAML conf
  YAML::Node conf = YAML::LoadFile("conf/kvs-config.yml");
  unsigned kRoutingThreadCount = conf["threads"]["routing"].as<unsigned>();

  YAML::Node user = conf["user"];
  Address ip = user["ip"].as<Address>();

  vector<Address> routing_ips;
  if (YAML::Node elb = user["routing-elb"]) {
    routing_ips.push_back(elb.as<Address>());
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
