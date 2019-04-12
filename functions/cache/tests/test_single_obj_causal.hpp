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

#include "causal_cache_handlers.hpp"
#include "causal_cache_utils.hpp"

TEST_F(CausalCacheTest, InsertToUnmergedStore) {
  EXPECT_EQ(unmerged_store.size(), 0);
  // populate unmerged store with 2 keys
  Key key = "a";
  VectorClock vc = construct_vector_clock({{"1", 2}, {"2", 2}});
  map<Key, VectorClock> dep;
  dep["b"] = construct_vector_clock({{"1", 1}, {"2", 1}});
  unmerged_store[key] = construct_causal_lattice(vc, dep, value);

  EXPECT_EQ(unmerged_store.size(), 1);

  key = "b";
  vc = construct_vector_clock({{"1", 1}, {"2", 2}});
  dep.clear();
  unmerged_store[key] = construct_causal_lattice(vc, dep, value);

  EXPECT_EQ(unmerged_store.size(), 2);
}

TEST_F(CausalCacheTest, SingleFullyCovered) {
  EXPECT_EQ(unmerged_store.size(), 0);
  // populate unmerged store with 2 keys
  Key key = "a";
  VectorClock vc = construct_vector_clock({{"1", 2}, {"2", 2}});
  map<Key, VectorClock> dep;
  dep["b"] = construct_vector_clock({{"1", 1}, {"2", 1}});
  unmerged_store[key] = construct_causal_lattice(vc, dep, value);

  EXPECT_EQ(unmerged_store.size(), 1);

  key = "b";
  vc = construct_vector_clock({{"1", 1}, {"2", 2}});
  dep.clear();
  unmerged_store[key] = construct_causal_lattice(vc, dep, value);

  EXPECT_EQ(unmerged_store.size(), 2);

  CausalRequest request;
  request.set_consistency(ConsistencyType::SINGLE);
  request.set_id("test");
  // add two keys
  CausalTuple* tp = request.add_tuples();
  tp->set_key("a");
  tp = request.add_tuples();
  tp->set_key("b");

  request.set_response_address(dummy_address);

  string serialized;
  request.SerializeToString(&serialized);

  get_request_handler(serialized, key_set, unmerged_store, in_preparation,
                      causal_cut_store, version_store, single_callback_map,
                      pending_single_metadata, pending_cross_metadata,
                      to_fetch_map, cover_map, pushers, client, log_, cct,
                      client_id_to_address_map);

  CausalResponse response;
  response.ParseFromString(mock_zmq_util.sent_messages[0]);

  set<Key> expected = {"a", "b"};
  set<Key> result;

  for (const CausalTuple& tp : response.tuples()) {
    result.insert(tp.key());
  }

  EXPECT_THAT(result, testing::UnorderedElementsAreArray(expected));
}

TEST_F(CausalCacheTest, SinglePartiallyCovered) {
  EXPECT_EQ(unmerged_store.size(), 0);
  // populate unmerged store with 2 keys
  Key key = "a";
  VectorClock vc = construct_vector_clock({{"1", 2}, {"2", 2}});
  map<Key, VectorClock> dep;
  unmerged_store[key] = construct_causal_lattice(vc, dep, value);

  EXPECT_EQ(unmerged_store.size(), 1);

  CausalRequest request;
  request.set_consistency(ConsistencyType::SINGLE);
  request.set_id("test");
  // add two keys
  CausalTuple* tp = request.add_tuples();
  tp->set_key("a");
  tp = request.add_tuples();
  tp->set_key("b");

  request.set_response_address(dummy_address);

  string serialized;
  request.SerializeToString(&serialized);

  get_request_handler(serialized, key_set, unmerged_store, in_preparation,
                      causal_cut_store, version_store, single_callback_map,
                      pending_single_metadata, pending_cross_metadata,
                      to_fetch_map, cover_map, pushers, client, log_, cct,
                      client_id_to_address_map);

  map<Key, set<Address>> expected_single_callback_map = {
      {"b", {dummy_address}}};

  map<Address, PendingClientMetadata> expected_pending_single_metadata = {
      {dummy_address, PendingClientMetadata("test", {"a", "b"}, {"b"})}};

  EXPECT_THAT(pending_single_metadata, testing::UnorderedElementsAreArray(
                                           expected_pending_single_metadata));
  EXPECT_THAT(single_callback_map,
              testing::UnorderedElementsAreArray(expected_single_callback_map));
  EXPECT_EQ(mock_cl.keys_get_.size(), 1);
  EXPECT_EQ(*(mock_cl.keys_get_.begin()), "b");
}

TEST_F(CausalCacheTest, SingleEndToEnd) {
  EXPECT_EQ(unmerged_store.size(), 0);
  // populate unmerged store with 2 keys
  Key key = "a";
  VectorClock vc = construct_vector_clock({{"1", 2}, {"2", 2}});
  map<Key, VectorClock> dep;
  unmerged_store[key] = construct_causal_lattice(vc, dep, value);

  EXPECT_EQ(unmerged_store.size(), 1);

  CausalRequest request;
  request.set_consistency(ConsistencyType::SINGLE);
  request.set_id("test");
  // add two keys
  CausalTuple* tp = request.add_tuples();
  tp->set_key("a");
  tp = request.add_tuples();
  tp->set_key("b");

  request.set_response_address(dummy_address);

  string serialized;
  request.SerializeToString(&serialized);

  get_request_handler(serialized, key_set, unmerged_store, in_preparation,
                      causal_cut_store, version_store, single_callback_map,
                      pending_single_metadata, pending_cross_metadata,
                      to_fetch_map, cover_map, pushers, client, log_, cct,
                      client_id_to_address_map);

  map<Key, set<Address>> expected_single_callback_map = {
      {"b", {dummy_address}}};

  map<Address, PendingClientMetadata> expected_pending_single_metadata = {
      {dummy_address, PendingClientMetadata("test", {"a", "b"}, {"b"})}};

  EXPECT_THAT(pending_single_metadata, testing::UnorderedElementsAreArray(
                                           expected_pending_single_metadata));
  EXPECT_THAT(single_callback_map,
              testing::UnorderedElementsAreArray(expected_single_callback_map));
  EXPECT_EQ(mock_cl.keys_get_.size(), 1);
  EXPECT_EQ(*(mock_cl.keys_get_.begin()), "b");

  // generate a mock KeyResponse for key "b"
  KeyResponse response;
  response.set_type(RequestType::GET);
  KeyTuple* ktp = response.add_tuples();
  ktp->set_key("b");
  ktp->set_error(0);
  vc = construct_vector_clock({{"1", 1}, {"2", 2}});
  dep.clear();
  ktp->set_payload(serialize(*construct_causal_lattice(vc, dep, value)));

  kvs_response_handler(response, unmerged_store, in_preparation,
                       causal_cut_store, version_store, single_callback_map,
                       pending_single_metadata, pending_cross_metadata,
                       to_fetch_map, cover_map, pushers, client, log_, cct,
                       client_id_to_address_map, request_id_to_address_map);

  CausalResponse causal_response;
  causal_response.ParseFromString(mock_zmq_util.sent_messages[0]);

  vector<Key> keys;
  vector<string> values;

  for (const CausalTuple& tp : causal_response.tuples()) {
    keys.push_back(tp.key());
    values.push_back(deserialize_cross_causal(tp.payload()).values(0));
  }

  EXPECT_THAT(keys, testing::UnorderedElementsAreArray({"a", "b"}));
  EXPECT_THAT(values, testing::UnorderedElementsAreArray({value, value}));

  expected_pending_single_metadata.clear();
  expected_single_callback_map.clear();

  EXPECT_THAT(pending_single_metadata, testing::UnorderedElementsAreArray(
                                           expected_pending_single_metadata));
  EXPECT_THAT(single_callback_map,
              testing::UnorderedElementsAreArray(expected_single_callback_map));
}