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

#include "kvs/kvs_handlers.hpp"

void cache_ip_response_handler(
  string& serialized, map<Address, set<Key>>& cache_ip_to_keys,
  map<Key, set<Address>>& key_to_cache_ips) {

  // The response will be a list of cache IPs and their responsible keys.
  KeyResponse response;
  response.ParseFromString(serialized);

  for (const auto& tuple : response.tuples()) {
    // tuple is a key-value pair from the KVS;
    // here, the key is the metadata key for the cache IP,
    // and the value is the list of keys that cache is responsible for.

    unsigned error = tuple.error();

    if (error == 0) {
      // Extract the cache IP.
      Address cache_ip = get_key_from_user_metadata(tuple.key());

      // Extract the keys that the cache is responsible for.
      LWWValue lww_value;
      lww_value.ParseFromString(tuple.payload());
      KeySet key_set;
      key_set.ParseFromString(lww_value.value());

      // First, update key_to_cache_ips with dropped keys for this cache.

      // Figure out which keys are in the old set of keys for this IP
      // that are not in the new fresh set of keys for this IP.
      // (We can do this by destructively modifying the old set of keys
      // since we don't need it anymore.)
      set<Key>& old_keys_for_ip = cache_ip_to_keys[cache_ip];
      for (const auto& cache_key : key_set.keys()) {
        old_keys_for_ip.erase(cache_key);
      }
      set<Key>& deleted_keys = old_keys_for_ip;

      // For the keys that have been deleted from this cache,
      // remove them from the key->caches mapping too.
      for (const auto& key : deleted_keys) {
        key_to_cache_ips[key].erase(cache_ip);
      }

      cache_ip_to_keys[cache_ip].clear();

      // Now we can update cache_ip_to_keys,
      // as well as add new keys to key_to_cache_ips.
      for (const auto& cache_key : key_set.keys()) {
        cache_ip_to_keys[cache_ip].emplace(std::move(cache_key));
        key_to_cache_ips[cache_key].insert(cache_ip);
      }
    }
    // We can also get error 1 (key does not exist)
    // or error 2 (node not responsible for key).
    // We just ignore these for now;
    // 1 means the cache has not told the kvs about any keys yet,
    // and 2 will be fixed on our next cached keys update interval.
  }
}
