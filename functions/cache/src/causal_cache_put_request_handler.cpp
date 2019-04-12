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

#include "causal_cache_utils.hpp"

void put_request_handler(const string& serialized, StoreType& unmerged_store,
                         StoreType& causal_cut_store,
                         VersionStoreType& version_store,
                         map<string, Address>& request_id_to_address_map,
                         KvsAsyncClientInterface* client) {
  CausalRequest request;
  request.ParseFromString(serialized);

  for (CausalTuple tuple : request.tuples()) {
    Key key = tuple.key();
    auto lattice = std::make_shared<CrossCausalLattice<SetLattice<string>>>(
        to_cross_causal_payload(deserialize_cross_causal(tuple.payload())));
    // first, update unmerged store
    if (unmerged_store.find(key) == unmerged_store.end()) {
      unmerged_store[key] = lattice;
    } else {
      unsigned comp_result = causal_comparison(unmerged_store[key], lattice);
      if (comp_result == kCausalLess) {
        unmerged_store[key] = lattice;
      } else if (comp_result == kCausalConcurrent) {
        unmerged_store[key] = causal_merge(unmerged_store[key], lattice);
      }
    }
    // if cross causal, also update causal cut
    if (request.consistency() == ConsistencyType::CROSS) {
      // we compare two lattices
      unsigned comp_result = causal_comparison(causal_cut_store[key], lattice);
      if (comp_result == kCausalLess) {
        causal_cut_store[key] = lattice;
      } else if (comp_result == kCausalConcurrent) {
        causal_cut_store[key] = causal_merge(causal_cut_store[key], lattice);
      }
      // keep this version
      version_store[request.id()][key] = lattice;
    }
    // write to KVS
    string req_id = client->put_async(key, serialize(*unmerged_store[key]),
                                      LatticeType::CROSSCAUSAL);
    request_id_to_address_map[req_id] = request.response_address();
  }
}