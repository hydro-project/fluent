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

#ifndef KVS_INCLUDE_HASH_RING_HPP_
#define KVS_INCLUDE_HASH_RING_HPP_

#include "common.hpp"
#include "consistent_hash_map.hpp"
#include "hashers.hpp"
#include "kvs_common.hpp"
#include "metadata.hpp"

template <typename H>
class HashRing : public ConsistentHashMap<ServerThread, H> {
 public:
  HashRing() {}

  ~HashRing() {}

 public:
  ServerThreadSet get_unique_servers() const { return unique_servers; }

  bool insert(Address public_ip, Address private_ip, int join_count,
              unsigned tid) {
    ServerThread new_thread = ServerThread(public_ip, private_ip, tid, 0);

    if (unique_servers.find(new_thread) != unique_servers.end()) {
      // if we already have the server, only return true if it's rejoining
      if (server_join_count[private_ip] < join_count) {
        server_join_count[private_ip] = join_count;
        return true;
      }

      return false;
    } else {  // otherwise, insert it into the hash ring for the first time
      unique_servers.insert(new_thread);
      server_join_count[private_ip] = join_count;

      for (unsigned virtual_num = 0; virtual_num < kVirtualThreadNum;
           virtual_num++) {
        ServerThread st = ServerThread(public_ip, private_ip, tid, virtual_num);
        ConsistentHashMap<ServerThread, H>::insert(st);
      }

      return true;
    }
  }

  void remove(Address public_ip, Address private_ip, unsigned tid) {
    for (unsigned virtual_num = 0; virtual_num < kVirtualThreadNum;
         virtual_num++) {
      ServerThread st = ServerThread(public_ip, private_ip, tid, virtual_num);
      ConsistentHashMap<ServerThread, H>::erase(st);
    }

    unique_servers.erase(ServerThread(public_ip, private_ip, tid, 0));
    server_join_count.erase(private_ip);
  }

 private:
  ServerThreadSet unique_servers;
  map<string, int> server_join_count;
};

typedef HashRing<GlobalHasher> GlobalHashRing;
typedef HashRing<LocalHasher> LocalHashRing;

class HashRingUtilInterface {
 public:
  virtual ServerThreadList get_responsible_threads(
      Address respond_address, const Key& key, bool metadata,
      map<TierId, GlobalHashRing>& global_hash_rings,
      map<TierId, LocalHashRing>& local_hash_rings,
      map<Key, KeyReplication>& key_replication_map, SocketCache& pushers,
      const vector<unsigned>& tier_ids, bool& succeed, unsigned& seed) = 0;

  ServerThreadList get_responsible_threads_metadata(
      const Key& key, GlobalHashRing& global_memory_hash_ring,
      LocalHashRing& local_memory_hash_ring);

  void issue_replication_factor_request(const Address& respond_address,
                                        const Key& key,
                                        GlobalHashRing& global_memory_hash_ring,
                                        LocalHashRing& local_memory_hash_ring,
                                        SocketCache& pushers, unsigned& seed);
};

class HashRingUtil : public HashRingUtilInterface {
 public:
  virtual ServerThreadList get_responsible_threads(
      Address respond_address, const Key& key, bool metadata,
      map<TierId, GlobalHashRing>& global_hash_rings,
      map<TierId, LocalHashRing>& local_hash_rings,
      map<Key, KeyReplication>& key_replication_map, SocketCache& pushers,
      const vector<unsigned>& tier_ids, bool& succeed, unsigned& seed);
};

ServerThreadList responsible_global(const Key& key, unsigned global_rep,
                                    GlobalHashRing& global_hash_ring);

set<unsigned> responsible_local(const Key& key, unsigned local_rep,
                                LocalHashRing& local_hash_ring);

Address prepare_metadata_request(const Key& key,
                                 GlobalHashRing& global_memory_hash_ring,
                                 LocalHashRing& local_memory_hash_ring,
                                 map<Address, KeyRequest>& addr_request_map,
                                 Address response_address, unsigned& rid,
                                 RequestType type);

void prepare_metadata_get_request(const Key& key,
                                  GlobalHashRing& global_memory_hash_ring,
                                  LocalHashRing& local_memory_hash_ring,
                                  map<Address, KeyRequest>& addr_request_map,
                                  Address response_address, unsigned& rid);

void prepare_metadata_put_request(const Key& key, const string& value,
                                  GlobalHashRing& global_memory_hash_ring,
                                  LocalHashRing& local_memory_hash_ring,
                                  map<Address, KeyRequest>& addr_request_map,
                                  Address response_address, unsigned& rid);

extern HashRingUtilInterface* kHashRingUtil;

#endif  // KVS_INCLUDE_HASH_RING_HPP_
