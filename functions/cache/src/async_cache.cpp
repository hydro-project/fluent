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
  PendingClientMetadata(set<Key> read_set, set<Key> to_cache_set) :
      read_set_(std::move(read_set)),
      to_cache_set_(std::move(to_cache_set)) {}
  set<Key> read_set_;
  set<Key> to_cache_set_;
};

string get_serialized_value_from_cache(
    const Key& key, LatticeType type,
    const map<Key, LWWPairLattice<string>>& local_lww_cache,
    const map<Key, SetLattice<string>>& local_set_cache, logger log) {
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
  } else {
    log->error("Invalid lattice type.");
    return "";
  }
}

void update_cache(const Key& key, LatticeType type, const string& payload,
                  map<Key, LWWPairLattice<string>>& local_lww_cache,
                  map<Key, SetLattice<string>>& local_set_cache, logger log) {
  if (type == LatticeType::LWW) {
    local_lww_cache[key].merge(deserialize_lww(payload));
  } else if (type == LatticeType::SET) {
    local_set_cache[key].merge(deserialize_set(payload));
  } else {
    log->error("Invalid lattice type.");
  }
}

void send_get_response(const set<Key>& read_set, const Address& response_addr,
                       const map<Key, LatticeType>& key_type_map,
                       const map<Key, LWWPairLattice<string>>& local_lww_cache,
                       const map<Key, SetLattice<string>>& local_set_cache,
                       SocketCache& pushers, logger log) {
  KeyResponse response;
  response.set_type(RequestType::GET);
  for (const Key& key : read_set) {
    KeyTuple* tp = response.add_tuples();
    tp->set_key(key);
    tp->set_lattice_type(key_type_map.at(key));
    tp->set_payload(get_serialized_value_from_cache(
        key, key_type_map.at(key), local_lww_cache, local_set_cache, log));
  }
  std::string resp_string;
  response.SerializeToString(&resp_string);
  kZmqUtil->send_string(resp_string, &pushers[response_addr]);
}

void send_error_response(RequestType type, const Address& response_addr,
                         SocketCache& pushers) {
  KeyResponse response;
  response.set_type(type);
  response.set_error_msg("LATTICE_ERROR");
  std::string resp_string;
  response.SerializeToString(&resp_string);
  kZmqUtil->send_string(resp_string, &pushers[response_addr]);
}

void run(KvsAsyncClient& client, Address ip, unsigned thread_id) {
  string log_file = "cache_log_" + std::to_string(thread_id) + ".txt";
  string log_name = "cache_log_" + std::to_string(thread_id);
  auto log = spdlog::basic_logger_mt(log_name, log_file, true);
  log->flush_on(spdlog::level::info);

  zmq::context_t context(1);
  SocketCache pushers(&context, ZMQ_PUSH);

  map<Key, LWWPairLattice<string>> local_lww_cache;
  map<Key, SetLattice<string>> local_set_cache;

  map<Address, PendingClientMetadata> pending_request_read_set;
  map<Key, set<Address>> key_requestor_map;

  map<Key, LatticeType> key_type_map;

  // mapping from request id to respond address of PUT request
  map<string, Address> request_address_map;

  CacheThread ct = CacheThread(ip, thread_id);

  // TODO: can we find a way to make the thread classes uniform across
  // languages? or unify the python and cpp implementations; actually, mostly
  // just the user thread stuff, I think.
  zmq::socket_t get_puller(context, ZMQ_PULL);
  get_puller.bind(ct.cache_get_bind_address());

  zmq::socket_t put_puller(context, ZMQ_PULL);
  put_puller.bind(ct.cache_put_bind_address());

  zmq::socket_t update_puller(context, ZMQ_PULL);
  update_puller.bind(ct.cache_update_bind_address());

  vector<zmq::pollitem_t> pollitems = {
      {static_cast<void*>(get_puller), 0, ZMQ_POLLIN, 0},
      {static_cast<void*>(put_puller), 0, ZMQ_POLLIN, 0},
      {static_cast<void*>(update_puller), 0, ZMQ_POLLIN, 0},
  };

  auto report_start = std::chrono::system_clock::now();
  auto report_end = std::chrono::system_clock::now();

  while (true) {
    kZmqUtil->poll(0, &pollitems);

    // handle a GET request
    if (pollitems[0].revents & ZMQ_POLLIN) {
      string serialized = kZmqUtil->recv_string(&get_puller);
      KeyRequest request;
      request.ParseFromString(serialized);

      bool covered = true;
      bool error = false;
      set<Key> read_set;
      set<Key> to_cover;

      for (KeyTuple tuple : request.tuples()) {
        Key key = tuple.key();
        read_set.insert(key);

        if (!tuple.has_lattice_type()) {
          log->error("Cache requires type to retrieve key.");
          send_error_response(RequestType::GET, request.response_address(),
                              pushers);
          error = true;
          break;
        } else if ((key_type_map.find(key) != key_type_map.end()) &&
                   (key_type_map[key] != tuple.lattice_type())) {
          log->error("lattice type mismatch.");
          send_error_response(RequestType::GET, request.response_address(),
                              pushers);
          error = true;
          break;
        }

        switch (tuple.lattice_type()) {
          case LatticeType::LWW: {
            if (local_lww_cache.find(key) == local_lww_cache.end()) {
              covered = false;
              to_cover.insert(key);
              key_requestor_map[key].insert(request.response_address());
              key_type_map[key] = LatticeType::LWW;
              client.get_async(key);
            }
            break;
          }
          case LatticeType::SET: {
            if (local_set_cache.find(key) == local_set_cache.end()) {
              covered = false;
              to_cover.insert(key);
              key_requestor_map[key].insert(request.response_address());
              key_type_map[key] = LatticeType::SET;
              client.get_async(key);
            }
            break;
          }
          default: break;
        }
      }

      if (!error) {
        if (covered) {
          send_get_response(read_set, request.response_address(), key_type_map,
                            local_lww_cache, local_set_cache, pushers, log);
        } else {
          pending_request_read_set[request.response_address()] =
              PendingClientMetadata(read_set, to_cover);
        }
      }
    }

    // handle a PUT request
    if (pollitems[1].revents & ZMQ_POLLIN) {
      string serialized = kZmqUtil->recv_string(&put_puller);
      KeyRequest request;
      request.ParseFromString(serialized);

      bool error = false;

      for (KeyTuple tuple : request.tuples()) {
        Key key = tuple.key();

        if (!tuple.has_lattice_type()) {
          log->error("Cache requires type to retrieve key.");
          send_error_response(RequestType::PUT, request.response_address(),
                              pushers);
          error = true;
          break;
        } else if ((key_type_map.find(key) != key_type_map.end()) &&
                   (key_type_map[key] != tuple.lattice_type())) {
          log->error("lattice type mismatch.");
          send_error_response(RequestType::PUT, request.response_address(),
                              pushers);
          error = true;
          break;
        }

        if (!error) {
          update_cache(key, tuple.lattice_type(), tuple.payload(),
                       local_lww_cache, local_set_cache, log);
          string req_id =
              client.put_async(key, tuple.payload(), tuple.lattice_type());
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

          log->error("lattice type mismatch during key update from KVS.");

          switch (key_type_map[key]) {
            case LatticeType::LWW: local_lww_cache.erase(key); break;
            case LatticeType::SET: local_set_cache.erase(key); break;
            default:
              break;  // this can never happen
          }

          key_type_map[key] = tuple.lattice_type();
        }

        update_cache(key, tuple.lattice_type(), tuple.payload(),
                     local_lww_cache, local_set_cache, log);
      }
    }

    vector<KeyResponse> responses = client.receive_async(kZmqUtil);
    for (const auto& response : responses) {
      Key key = response.tuples(0).key();
      // TODO: assert that key_type_map[key] and
      // response.tuples(0).lattice_type() are the same first, check if
      // the request failed
      if (response.has_error_msg() && response.error_msg() == "NULL_ERROR") {
        if (response.type() == RequestType::GET) {
          client.get_async(key);
        } else {
          if (request_address_map.find(response.response_id()) !=
              request_address_map.end()) {
            // we only retry for client-issued requests, not for the periodic
            // stat report
            string new_req_id =
                client.put_async(key, response.tuples(0).payload(),
                                 response.tuples(0).lattice_type());
            request_address_map[new_req_id] =
                request_address_map[response.response_id()];
            // GC the original request_id address pair
            request_address_map.erase(response.response_id());
          }
        }
      } else {
        if (response.type() == RequestType::GET) {
          // update cache first
          update_cache(key, response.tuples(0).lattice_type(),
                       response.tuples(0).payload(), local_lww_cache,
                       local_set_cache, log);
          // notify clients
          if (key_requestor_map.find(key) != key_requestor_map.end()) {
            for (const Address& addr : key_requestor_map[key]) {
              pending_request_read_set[addr].to_cache_set_.erase(key);
              if (pending_request_read_set[addr].to_cache_set_.size() == 0) {
                // all keys covered
                send_get_response(pending_request_read_set[addr].read_set_,
                                  addr, key_type_map, local_lww_cache,
                                  local_set_cache, pushers, log);
                pending_request_read_set.erase(addr);
              }
            }
            key_requestor_map.erase(key);
          }
        } else {
          // forward the PUT response to client
          if (request_address_map.find(response.response_id()) ==
              request_address_map.end()) {
            log->error(
                "Missing request id - address entry for this PUT response");
          } else {
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
      client.put_async(key, serialize(val), LatticeType::LWW);
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

  KvsAsyncClient client(threads, ip, 0, 10000);

  run(client, ip, 0);
}
