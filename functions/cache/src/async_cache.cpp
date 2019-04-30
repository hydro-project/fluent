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

#include "kvs_async_client.hpp"
#include "yaml-cpp/yaml.h"

ZmqUtil zmq_util;
ZmqUtilInterface* kZmqUtil = &zmq_util;

unsigned kCacheReportThreshold = 5;

struct PendingClientMetadata {
  PendingClientMetadata() = default;
  PendingClientMetadata(set<Key> read_set, set<Key> to_retrieve_set) :
      read_set_(std::move(read_set)),
      to_retrieve_set_(std::move(to_retrieve_set)) {}
  set<Key> read_set_;
  set<Key> to_retrieve_set_;
};

string get_serialized_value_from_cache(
    const Key& key, LatticeType type,
    const map<Key, LWWPairLattice<string>>& local_lww_cache,
    const map<Key, SetLattice<string>>& local_set_cache,
    const map<Key, OrderedSetLattice<string>>& local_ordered_set_cache,
    logger log) {
  if (type == LatticeType::LWW) {
    if (local_lww_cache.find(key) != local_lww_cache.end()) {
      return serialize(local_lww_cache.at(key));
    } else {
      log->error("Key {} not found in LWW cache.", key);
      return "";
    }
  } else if (type == LatticeType::SET) {
    if (local_set_cache.find(key) != local_set_cache.end()) {
      return serialize(local_set_cache.at(key));
    } else {
      log->error("Key {} not found in SET cache.", key);
      return "";
    }
  } else if (type == LatticeType::ORDERED_SET) {
    if (local_ordered_set_cache.find(key) != local_ordered_set_cache.end()) {
      return serialize(local_ordered_set_cache.at(key));
    } else {
      log->error("Key {} not found in ORDERED_SET cache.", key);
      return "";
    }
  } else {
    log->error("Invalid lattice type.");
    return "";
  }
}

void update_cache(const Key& key, LatticeType type, const string& payload,
                  map<Key, LWWPairLattice<string>>& local_lww_cache,
                  map<Key, SetLattice<string>>& local_set_cache,
                  map<Key, OrderedSetLattice<string>>& local_ordered_set_cache,
                  logger log) {
  if (type == LatticeType::LWW) {
    local_lww_cache[key].merge(deserialize_lww(payload));
  } else if (type == LatticeType::SET) {
    local_set_cache[key].merge(deserialize_set(payload));
  } else if (type == LatticeType::ORDERED_SET) {
    local_ordered_set_cache[key].merge(deserialize_ordered_set(payload));
  } else {
    log->error("Invalid lattice type.");
  }
}

void send_get_response(
    const set<Key>& read_set, const Address& response_addr,
    const map<Key, LatticeType>& key_type_map,
    const map<Key, LWWPairLattice<string>>& local_lww_cache,
    const map<Key, SetLattice<string>>& local_set_cache,
    const map<Key, OrderedSetLattice<string>>& local_ordered_set_cache,
    SocketCache& pushers, logger log) {
  KeyResponse response;
  response.set_type(RequestType::GET);

  for (const Key& key : read_set) {
    KeyTuple* tp = response.add_tuples();
    tp->set_key(key);
    if (key_type_map.find(key) == key_type_map.end()) {
      // key dne in cache, it actually means that there is a
      // response from kvs that has error = 1
      tp->set_error(1);
    } else {
      tp->set_error(0);
      tp->set_lattice_type(key_type_map.at(key));
      tp->set_payload(get_serialized_value_from_cache(
          key, key_type_map.at(key), local_lww_cache, local_set_cache,
          local_ordered_set_cache, log));
    }
  }

  std::string resp_string;
  response.SerializeToString(&resp_string);
  kZmqUtil->send_string(resp_string, &pushers[response_addr]);
}

void send_error_response(RequestType type, const Address& response_addr,
                         SocketCache& pushers) {
  KeyResponse response;
  response.set_type(type);
  response.set_error(ResponseErrorType::LATTICE);
  std::string resp_string;
  response.SerializeToString(&resp_string);
  kZmqUtil->send_string(resp_string, &pushers[response_addr]);
}

void run(KvsAsyncClientInterface* client, Address ip, unsigned thread_id) {
  string log_file = "cache_log_" + std::to_string(thread_id) + ".txt";
  string log_name = "cache_log_" + std::to_string(thread_id);
  auto log = spdlog::basic_logger_mt(log_name, log_file, true);
  log->flush_on(spdlog::level::info);

  zmq::context_t* context = client->get_context();

  SocketCache pushers(context, ZMQ_PUSH);

  map<Key, LWWPairLattice<string>> local_lww_cache;
  map<Key, SetLattice<string>> local_set_cache;
  map<Key, OrderedSetLattice<string>> local_ordered_set_cache;

  map<Address, PendingClientMetadata> pending_request_read_set;
  map<Key, set<Address>> key_requestor_map;

  map<Key, LatticeType> key_type_map;

  // mapping from request id to respond address of PUT request
  map<string, Address> request_address_map;

  CacheThread ct = CacheThread(ip, thread_id);

  // TODO: can we find a way to make the thread classes uniform across
  // languages? or unify the python and cpp implementations; actually, mostly
  // just the user thread stuff, I think.
  zmq::socket_t get_puller(*context, ZMQ_PULL);
  get_puller.bind(ct.cache_get_bind_address());

  zmq::socket_t put_puller(*context, ZMQ_PULL);
  put_puller.bind(ct.cache_put_bind_address());

  zmq::socket_t update_puller(*context, ZMQ_PULL);
  update_puller.bind(ct.cache_update_bind_address());

  vector<zmq::pollitem_t> pollitems = {
      {static_cast<void*>(get_puller), 0, ZMQ_POLLIN, 0},
      {static_cast<void*>(put_puller), 0, ZMQ_POLLIN, 0},
      {static_cast<void*>(update_puller), 0, ZMQ_POLLIN, 0},
  };

  auto report_start = std::chrono::system_clock::now();
  auto report_end = std::chrono::system_clock::now();

  std::list<Key> access_order;
  map<Key, std::list<Key>::iterator> iterator_cache;

  while (true) {
    kZmqUtil->poll(0, &pollitems);

    // handle a GET request
    if (pollitems[0].revents & ZMQ_POLLIN) {
      string serialized = kZmqUtil->recv_string(&get_puller);
      KeyRequest request;
      request.ParseFromString(serialized);

      bool covered = true;
      set<Key> read_set;
      set<Key> to_retrieve;

      for (KeyTuple tuple : request.tuples()) {
        Key key = tuple.key();
        read_set.insert(key);

        if (key_type_map.find(key) == key_type_map.end()) {
          // this means key dne in cache
          covered = false;
          to_retrieve.insert(key);
          key_requestor_map[key].insert(request.response_address());
          client->get_async(key);
        } else {
          access_order.erase(iterator_cache[key]);
        }

        access_order.push_front(key);
        iterator_cache[key] = access_order.begin();
      }

      if (covered) {
        send_get_response(read_set, request.response_address(), key_type_map,
                          local_lww_cache, local_set_cache,
                          local_ordered_set_cache, pushers, log);
      } else {
        pending_request_read_set[request.response_address()] =
            PendingClientMetadata(read_set, to_retrieve);
      }
    }

    // handle a PUT request
    if (pollitems[1].revents & ZMQ_POLLIN) {
      string serialized = kZmqUtil->recv_string(&put_puller);
      KeyRequest request;
      request.ParseFromString(serialized);

      bool error = false;

      for (KeyTuple tuple : request.tuples()) {
        // this loop checks if any key has invalid lattice type
        Key key = tuple.key();

        if (!tuple.has_lattice_type()) {
          log->error("The cache requires the lattice type to PUT key.");
          send_error_response(RequestType::PUT, request.response_address(),
                              pushers);
          error = true;
          break;
        } else if ((key_type_map.find(key) != key_type_map.end()) &&
                   (key_type_map[key] != tuple.lattice_type())) {
          log->error(
              "Key {}: Lattice type for PUT does not match stored lattice "
              "type.",
              key);
          send_error_response(RequestType::PUT, request.response_address(),
                              pushers);
          error = true;
          break;
        }
      }

      if (!error) {
        for (KeyTuple tuple : request.tuples()) {
          Key key = tuple.key();

          // first update key type map
          key_type_map[key] = tuple.lattice_type();
          update_cache(key, tuple.lattice_type(), tuple.payload(),
                       local_lww_cache, local_set_cache,
                       local_ordered_set_cache, log);

          if (iterator_cache.find(key) != iterator_cache.end()) {
            access_order.erase(iterator_cache[key]);
          }

          access_order.push_front(key);
          iterator_cache[key] = access_order.begin();

          string req_id =
              client->put_async(key, tuple.payload(), tuple.lattice_type());
          request_address_map[req_id] = request.response_address();
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

          log->error(
              "Key {}: Stored lattice type did not match type received "
              "in KVS update.",
              key);

          switch (key_type_map[key]) {
            case LatticeType::LWW: local_lww_cache.erase(key); break;
            case LatticeType::SET: local_set_cache.erase(key); break;
            case LatticeType::ORDERED_SET:
              local_ordered_set_cache.erase(key);
              break;
            default: break;  // this can never happen
          }

          key_type_map[key] = tuple.lattice_type();
        }

        update_cache(key, tuple.lattice_type(), tuple.payload(),
                     local_lww_cache, local_set_cache, local_ordered_set_cache,
                     log);
      }
    }

    vector<KeyResponse> responses = client->receive_async(kZmqUtil);
    for (const auto& response : responses) {
      Key key = response.tuples(0).key();

      if (response.has_error() &&
          response.error() == ResponseErrorType::TIMEOUT) {
        if (response.type() == RequestType::GET) {
          client->get_async(key);
        } else {
          if (request_address_map.find(response.response_id()) !=
              request_address_map.end()) {
            // we only retry for client-issued requests, not for the periodic
            // stat report
            string new_req_id =
                client->put_async(key, response.tuples(0).payload(),
                                  response.tuples(0).lattice_type());

            request_address_map[new_req_id] =
                request_address_map[response.response_id()];
            request_address_map.erase(response.response_id());
          }
        }
      } else {
        if (response.type() == RequestType::GET) {
          // update cache first
          if (response.tuples(0).error() != 1) {
            // we actually got a non null key
            key_type_map[key] = response.tuples(0).lattice_type();
            update_cache(key, response.tuples(0).lattice_type(),
                         response.tuples(0).payload(), local_lww_cache,
                         local_set_cache, local_ordered_set_cache, log);
          }

          // notify clients
          if (key_requestor_map.find(key) != key_requestor_map.end()) {
            for (const Address& addr : key_requestor_map[key]) {
              pending_request_read_set[addr].to_retrieve_set_.erase(key);

              if (pending_request_read_set[addr].to_retrieve_set_.size() == 0) {
                // all keys covered
                send_get_response(pending_request_read_set[addr].read_set_,
                                  addr, key_type_map, local_lww_cache,
                                  local_set_cache, local_ordered_set_cache,
                                  pushers, log);
                pending_request_read_set.erase(addr);
              }
            }

            key_requestor_map.erase(key);
          }
        } else {
          // we only send a response if we have a response address -- e.g., we
          // don't have one for updating our own cached key set
          if (request_address_map.find(response.response_id()) !=
              request_address_map.end()) {
            string resp_string;
            response.SerializeToString(&resp_string);

            kZmqUtil->send_string(
                resp_string,
                &pushers[request_address_map[response.response_id()]]);
            request_address_map.erase(response.response_id());
          }
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
      client->put_async(key, serialize(val), LatticeType::LWW);
      report_start = std::chrono::system_clock::now();
    }

    if (key_type_map.size() > 1000) {
      // drop the 10 least recently accessed keys
      for (int i = 0; i < 10; i++) {
        Key key = access_order.back();
        access_order.pop_back();
        iterator_cache.erase(key);

        LatticeType type = key_type_map[key];
        key_type_map.erase(key);

        if (type == LWW) {
          local_lww_cache.erase(key);
        } else if (type == SET) {
          local_set_cache.erase(key);
        } else if (type == ORDERED_SET) {
          local_ordered_set_cache.erase(key);
        }
      }
    }
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

  KvsAsyncClient cl(threads, ip, 0, 10000);
  KvsAsyncClientInterface* client = &cl;

  run(client, ip, 0);
}
