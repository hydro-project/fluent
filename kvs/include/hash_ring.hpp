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

#ifndef SRC_INCLUDE_HASH_RING_HPP_
#define SRC_INCLUDE_HASH_RING_HPP_

#include "common.hpp"
#include "hashers.hpp"
#include "metadata.hpp"
#include "utils/consistent_hash_map.hpp"

template <typename H>
class HashRing : public ConsistentHashMap<ServerThread, H> {
 public:
  HashRing() {}

  ~HashRing() {}

 public:
  std::unordered_set<ServerThread, ThreadHash> get_unique_servers() {
    return unique_servers;
  }

  bool insert(Address ip, unsigned tid) {
    bool succeed;
    for (unsigned virtual_num = 0; virtual_num < kVirtualThreadNum;
         virtual_num++) {
      ServerThread st = ServerThread(ip, tid, virtual_num);
      succeed = ConsistentHashMap<ServerThread, H>::insert(st).second;
      if (succeed) {
        unique_servers.insert(st);
      }
    }
    return succeed;
  }

  void remove(Address ip, unsigned tid) {
    for (unsigned virtual_num = 0; virtual_num < kVirtualThreadNum;
         virtual_num++) {
      ServerThread st = ServerThread(ip, tid, virtual_num);
      unique_servers.erase(st);
      ConsistentHashMap<ServerThread, H>::erase(st);
    }
  }

 private:
  std::unordered_set<ServerThread, ThreadHash> unique_servers;
};

typedef HashRing<GlobalHasher> GlobalHashRing;
typedef HashRing<LocalHasher> LocalHashRing;

class HashRingUtilInterface {
 public:
  virtual ServerThreadSet get_responsible_threads(
      Address respond_address, const Key& key, bool metadata,
      std::unordered_map<unsigned, GlobalHashRing>& global_hash_ring_map,
      std::unordered_map<unsigned, LocalHashRing>& local_hash_ring_map,
      std::unordered_map<Key, KeyInfo>& placement, SocketCache& pushers,
      const std::vector<unsigned>& tier_ids, bool& succeed, unsigned& seed) = 0;

  ServerThreadSet get_responsible_threads_metadata(
      const Key& key, GlobalHashRing& global_memory_hash_ring,
      LocalHashRing& local_memory_hash_ring);

  void issue_replication_factor_request(const Address& respond_address,
                                        const Key& key,
                                        GlobalHashRing& global_memory_hash_ring,
                                        LocalHashRing& local_memory_hash_ring,
                                        SocketCache& pushers, unsigned& seed);

  std::vector<Address> get_address_from_routing(UserThread& ut, const Key& key,
                                                zmq::socket_t& sending_socket,
                                                zmq::socket_t& receiving_socket,
                                                bool& succeed, Address& ip,
                                                unsigned& thread_id,
                                                unsigned& rid);

  RoutingThread get_random_routing_thread(std::vector<Address>& routing_address,
                                          unsigned& seed,
                                          unsigned& kRoutingThreadCount);
};

class HashRingUtil : public HashRingUtilInterface {
 public:
  virtual ServerThreadSet get_responsible_threads(
      Address respond_address, const Key& key, bool metadata,
      std::unordered_map<unsigned, GlobalHashRing>& global_hash_ring_map,
      std::unordered_map<unsigned, LocalHashRing>& local_hash_ring_map,
      std::unordered_map<Key, KeyInfo>& placement, SocketCache& pushers,
      const std::vector<unsigned>& tier_ids, bool& succeed, unsigned& seed);
};

ServerThreadSet responsible_global(const Key& key, unsigned global_rep,
                                   GlobalHashRing& global_hash_ring);

std::unordered_set<unsigned> responsible_local(const Key& key,
                                               unsigned local_rep,
                                               LocalHashRing& local_hash_ring);

extern HashRingUtilInterface* kHashRingUtil;

#endif  // SRC_INCLUDE_HASH_RING_HPP_
