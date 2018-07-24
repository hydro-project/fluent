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

#include "hash_ring.hpp"

#include <unistd.h>

#include "requests.hpp"

// get all threads responsible for a key from the "node_type" tier
// metadata flag = 0 means the key is  metadata; otherwise, it is  regular data
ServerThreadSet HashRingUtil::get_responsible_threads(
    Address response_address, const Key& key, bool metadata,
    std::unordered_map<unsigned, GlobalHashRing>& global_hash_ring_map,
    std::unordered_map<unsigned, LocalHashRing>& local_hash_ring_map,
    std::unordered_map<Key, KeyInfo>& placement, SocketCache& pushers,
    const std::vector<unsigned>& tier_ids, bool& succeed, unsigned& seed) {
  if (metadata) {
    succeed = true;
    return kHashRingUtil->get_responsible_threads_metadata(
        key, global_hash_ring_map[1], local_hash_ring_map[1]);
  } else {
    ServerThreadSet result;

    if (placement.find(key) == placement.end()) {
      kHashRingUtil->issue_replication_factor_request(
          response_address, key, global_hash_ring_map[1],
          local_hash_ring_map[1], pushers, seed);
      succeed = false;
    } else {
      for (const unsigned& tier_id : tier_ids) {
        ServerThreadSet threads = responsible_global(
            key, kMetadataReplicationFactor, global_hash_ring_map[tier_id]);

        for (const ServerThread& thread : threads) {
          Address ip = thread.get_ip();
          std::unordered_set<unsigned> tids = responsible_local(
              key, placement[key].local_replication_map_[tier_id],
              local_hash_ring_map[tier_id]);

          for (const unsigned& tid : tids) {
            result.insert(ServerThread(ip, tid));
          }
        }
      }

      succeed = true;
    }

    return result;
  }
}

// assuming the replication factor will never be greater than the number of
// nodes in a tier return a set of ServerThreads that are responsible for a key
ServerThreadSet responsible_global(const Key& key, unsigned global_rep,
                                   GlobalHashRing& global_hash_ring) {
  ServerThreadSet threads;
  auto pos = global_hash_ring.find(key);

  if (pos != global_hash_ring.end()) {
    // iterate for every value in the replication factor
    unsigned i = 0;

    while (i < global_rep) {
      bool succeed = threads.insert(pos->second).second;
      if (++pos == global_hash_ring.end()) {
        pos = global_hash_ring.begin();
      }

      if (succeed) {
        i += 1;
      }
    }
  }

  return threads;
}

// assuming the replication factor will never be greater than the number of
// worker threads return a set of tids that are responsible for a key
std::unordered_set<unsigned> responsible_local(const Key& key,
                                               unsigned local_rep,
                                               LocalHashRing& local_hash_ring) {
  std::unordered_set<unsigned> tids;
  auto pos = local_hash_ring.find(key);

  if (pos != local_hash_ring.end()) {
    // iterate for every value in the replication factor
    unsigned i = 0;

    while (i < local_rep) {
      bool succeed = tids.insert(pos->second.get_tid()).second;
      if (++pos == local_hash_ring.end()) {
        pos = local_hash_ring.begin();
      }

      if (succeed) {
        i += 1;
      }
    }
  }

  return tids;
}

ServerThreadSet HashRingUtilInterface::get_responsible_threads_metadata(
    const Key& key, GlobalHashRing& global_memory_hash_ring,
    LocalHashRing& local_memory_hash_ring) {
  ServerThreadSet threads = responsible_global(key, kMetadataReplicationFactor,
                                               global_memory_hash_ring);

  for (const ServerThread& thread : threads) {
    Address ip = thread.get_ip();
    std::unordered_set<unsigned> tids = responsible_local(
        key, kDefaultLocalReplication, local_memory_hash_ring);

    for (const unsigned& tid : tids) {
      threads.insert(ServerThread(ip, tid));
    }
  }

  return threads;
}

void HashRingUtilInterface::issue_replication_factor_request(
    const Address& response_address, const Key& key,
    GlobalHashRing& global_memory_hash_ring,
    LocalHashRing& local_memory_hash_ring, SocketCache& pushers,
    unsigned& seed) {
  Key replication_key = get_metadata_key(key, MetadataType::replication);
  auto threads = kHashRingUtil->get_responsible_threads_metadata(
      replication_key, global_memory_hash_ring, local_memory_hash_ring);

  Address target_address = next(begin(threads), rand_r(&seed) % threads.size())
                               ->get_request_pulling_connect_addr();

  KeyRequest key_request;
  key_request.set_type(get_request_type("GET"));
  key_request.set_response_address(response_address);

  prepare_get_tuple(key_request, replication_key);
  std::string serialized;
  key_request.SerializeToString(&serialized);
  kZmqUtil->send_string(serialized, &pushers[target_address]);
}

// query the routing for a key and return all address
std::vector<Address> HashRingUtilInterface::get_address_from_routing(
    UserThread& ut, const Key& key, zmq::socket_t& sending_socket,
    zmq::socket_t& receiving_socket, bool& succeed, Address& ip,
    unsigned& thread_id, unsigned& rid) {
  int count = 0;

  KeyAddressRequest address_request;
  KeyAddressResponse address_response;
  address_request.set_response_address(ut.get_key_address_connect_addr());
  address_request.add_keys(key);

  std::string req_id =
      ip + ":" + std::to_string(thread_id) + "_" + std::to_string(rid);
  address_request.set_request_id(req_id);
  std::vector<Address> result;

  int error = -1;

  while (error != 0) {
    if (error == 1) {
      std::cerr << "No servers have joined the cluster yet. Retrying request."
                << std::endl;
    }

    if (count > 0 && count % 5 == 0) {
      std::cerr
          << "Pausing for 5 seconds before continuing to query routing layer..."
          << std::endl;
      usleep(5000000);
    }

    rid += 1;

    address_response = send_request<KeyAddressRequest, KeyAddressResponse>(
        address_request, sending_socket, receiving_socket, succeed);

    if (!succeed) {
      return result;
    } else {
      error = address_response.error();
    }

    count++;
  }

  for (const std::string& ip : address_response.addresses(0).ips()) {
    result.push_back(ip);
  }

  return result;
}

RoutingThread HashRingUtilInterface::get_random_routing_thread(
    std::vector<Address>& routing_address, unsigned& seed,
    unsigned& kRoutingThreadCount) {
  Address routing_ip = routing_address[rand_r(&seed) % routing_address.size()];
  unsigned tid = rand_r(&seed) % kRoutingThreadCount;
  return RoutingThread(routing_ip, tid);
}
