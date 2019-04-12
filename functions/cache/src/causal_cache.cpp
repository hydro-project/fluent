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

#include "yaml-cpp/yaml.h"

#include "causal_cache_handlers.hpp"
#include "causal_cache_utils.hpp"

ZmqUtil zmq_util;
ZmqUtilInterface* kZmqUtil = &zmq_util;

void run(KvsAsyncClientInterface* client, Address ip, unsigned thread_id) {
  string log_file = "causal_cache_log_" + std::to_string(thread_id) + ".txt";
  string log_name = "causal_cache_log_" + std::to_string(thread_id);
  auto log = spdlog::basic_logger_mt(log_name, log_file, true);
  log->flush_on(spdlog::level::info);

  zmq::context_t* context = client->get_context();

  SocketCache pushers(context, ZMQ_PUSH);

  // keep track of keys that this causal cache is responsible for
  set<Key> key_set;

  StoreType unmerged_store;
  InPreparationType in_preparation;
  StoreType causal_cut_store;
  VersionStoreType version_store;

  map<Key, set<Key>> to_fetch_map;
  map<Key, std::unordered_map<VectorClock, set<Key>, VectorClockHash>>
      cover_map;

  map<Key, set<Address>> single_callback_map;

  map<Address, PendingClientMetadata> pending_single_metadata;
  map<Address, PendingClientMetadata> pending_cross_metadata;

  // mapping from client id to a set of response address of GET request
  map<string, set<Address>> client_id_to_address_map;

  // mapping from request id to response address of PUT request
  map<string, Address> request_id_to_address_map;

  CausalCacheThread cct = CausalCacheThread(ip, thread_id);

  // TODO: can we find a way to make the thread classes uniform across
  // languages? or unify the python and cpp implementations; actually, mostly
  // just the user thread stuff, I think.
  zmq::socket_t get_puller(*context, ZMQ_PULL);
  get_puller.bind(cct.causal_cache_get_bind_address());

  zmq::socket_t put_puller(*context, ZMQ_PULL);
  put_puller.bind(cct.causal_cache_put_bind_address());

  zmq::socket_t update_puller(*context, ZMQ_PULL);
  update_puller.bind(cct.causal_cache_update_bind_address());

  zmq::socket_t version_gc_puller(*context, ZMQ_PULL);
  version_gc_puller.bind(cct.causal_cache_version_gc_bind_address());

  zmq::socket_t versioned_key_request_puller(*context, ZMQ_PULL);
  versioned_key_request_puller.bind(
      cct.causal_cache_versioned_key_request_bind_address());

  zmq::socket_t versioned_key_response_puller(*context, ZMQ_PULL);
  versioned_key_response_puller.bind(
      cct.causal_cache_versioned_key_response_bind_address());

  vector<zmq::pollitem_t> pollitems = {
      {static_cast<void*>(get_puller), 0, ZMQ_POLLIN, 0},
      {static_cast<void*>(put_puller), 0, ZMQ_POLLIN, 0},
      {static_cast<void*>(update_puller), 0, ZMQ_POLLIN, 0},
      {static_cast<void*>(version_gc_puller), 0, ZMQ_POLLIN, 0},
      {static_cast<void*>(versioned_key_request_puller), 0, ZMQ_POLLIN, 0},
      {static_cast<void*>(versioned_key_response_puller), 0, ZMQ_POLLIN, 0},
  };

  auto report_start = std::chrono::system_clock::now();
  auto report_end = std::chrono::system_clock::now();

  auto migrate_start = std::chrono::system_clock::now();
  auto migrate_end = std::chrono::system_clock::now();

  while (true) {
    kZmqUtil->poll(0, &pollitems);

    // handle a GET request
    if (pollitems[0].revents & ZMQ_POLLIN) {
      string serialized = kZmqUtil->recv_string(&get_puller);
      get_request_handler(serialized, key_set, unmerged_store, in_preparation,
                          causal_cut_store, version_store, single_callback_map,
                          pending_single_metadata, pending_cross_metadata,
                          to_fetch_map, cover_map, pushers, client, log, cct,
                          client_id_to_address_map);
    }

    // handle a PUT request
    if (pollitems[1].revents & ZMQ_POLLIN) {
      string serialized = kZmqUtil->recv_string(&put_puller);
      put_request_handler(serialized, unmerged_store, causal_cut_store,
                          version_store, request_id_to_address_map, client);
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
        if (key_set.find(key) == key_set.end()) {
          continue;
        }

        auto lattice = std::make_shared<CrossCausalLattice<SetLattice<string>>>(
            to_cross_causal_payload(deserialize_cross_causal(tuple.payload())));

        process_response(key, lattice, unmerged_store, in_preparation,
                         causal_cut_store, version_store, single_callback_map,
                         pending_single_metadata, pending_cross_metadata,
                         to_fetch_map, cover_map, pushers, client, log, cct,
                         client_id_to_address_map);
      }
    }

    // handle version GC request
    if (pollitems[3].revents & ZMQ_POLLIN) {
      // assume this string is the client id
      string serialized = kZmqUtil->recv_string(&version_gc_puller);
      version_store.erase(serialized);
    }

    // handle versioned key request
    if (pollitems[4].revents & ZMQ_POLLIN) {
      string serialized = kZmqUtil->recv_string(&versioned_key_request_puller);
      versioned_key_request_handler(serialized, version_store, pushers, log,
                                    kZmqUtil);
    }

    // handle versioned key response
    if (pollitems[5].revents & ZMQ_POLLIN) {
      string serialized = kZmqUtil->recv_string(&versioned_key_response_puller);
      versioned_key_response_handler(
          serialized, causal_cut_store, version_store, pending_cross_metadata,
          client_id_to_address_map, cct, pushers, kZmqUtil);
    }

    vector<KeyResponse> responses = client->receive_async(kZmqUtil);
    for (const auto& response : responses) {
      kvs_response_handler(response, unmerged_store, in_preparation,
                           causal_cut_store, version_store, single_callback_map,
                           pending_single_metadata, pending_cross_metadata,
                           to_fetch_map, cover_map, pushers, client, log, cct,
                           client_id_to_address_map, request_id_to_address_map);
    }

    // collect and store internal statistics
    report_end = std::chrono::system_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::seconds>(
                        report_end - report_start)
                        .count();

    // update KVS with information about which keys this node is currently
    // caching; we only do this periodically because we are okay with receiving
    // potentially stale updates
    if (duration >= kCausalCacheReportThreshold) {
      KeySet set;

      for (const auto& pair : unmerged_store) {
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

    migrate_end = std::chrono::system_clock::now();
    duration = std::chrono::duration_cast<std::chrono::seconds>(migrate_end -
                                                                migrate_start)
                   .count();

    // check if any key in unmerged_store is newer and migrate
    if (duration >= kMigrateThreshold) {
      periodic_migration_handler(
          unmerged_store, in_preparation, causal_cut_store, version_store,
          pending_cross_metadata, to_fetch_map, cover_map, pushers, client, cct,
          client_id_to_address_map);
      migrate_start = std::chrono::system_clock::now();
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

  KvsAsyncClient cl(threads, ip, 0, 10000);
  KvsAsyncClientInterface* client = &cl;

  run(client, ip, 0);
}