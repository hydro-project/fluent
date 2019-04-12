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

TEST_F(CausalCacheTest, CrossFullyCovered) {
  EXPECT_EQ(causal_cut_store.size(), 0);
  // populate causal_cut_store with 2 keys with no dependency
  Key key = "a";
  VectorClock vc = construct_vector_clock({{"c1", 1}, {"c2", 1}});
  map<Key, VectorClock> dep;
  causal_cut_store[key] = construct_causal_lattice(vc, dep, value);

  EXPECT_EQ(causal_cut_store.size(), 1);

  key = "b";
  vc = construct_vector_clock({{"c1", 3}, {"c2", 2}});
  causal_cut_store[key] = construct_causal_lattice(vc, dep, value);

  EXPECT_EQ(causal_cut_store.size(), 2);

  // issue GET request
  CausalRequest request;
  request.set_consistency(ConsistencyType::CROSS);
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

  vector<Key> keys_expected = {"a", "b"};
  vector<string> values_expected = {value, value};

  vector<Key> keys_result;
  vector<string> values_result;

  for (const CausalTuple& tp : response.tuples()) {
    keys_result.push_back(tp.key());
    values_result.push_back(deserialize_cross_causal(tp.payload()).values(0));
  }

  EXPECT_THAT(keys_result, testing::UnorderedElementsAreArray(keys_expected));
  EXPECT_THAT(values_result,
              testing::UnorderedElementsAreArray(values_expected));
}

TEST_F(CausalCacheTest, CrossCoveredInUnmerged) {
  // populate unmerged_store with 2 keys with no dependency
  Key key = "a";
  VectorClock vc = construct_vector_clock({{"c1", 1}, {"c2", 1}});
  map<Key, VectorClock> dep;
  unmerged_store[key] = construct_causal_lattice(vc, dep, value);

  key = "b";
  vc = construct_vector_clock({{"c1", 3}, {"c2", 2}});
  unmerged_store[key] = construct_causal_lattice(vc, dep, value);

  // issue GET request
  CausalRequest request;
  request.set_consistency(ConsistencyType::CROSS);
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

  vector<Key> keys_expected = {"a", "b"};
  vector<string> values_expected = {value, value};

  vector<Key> keys_result;
  vector<string> values_result;

  for (const CausalTuple& tp : response.tuples()) {
    keys_result.push_back(tp.key());
    values_result.push_back(deserialize_cross_causal(tp.payload()).values(0));
  }

  EXPECT_THAT(keys_result, testing::UnorderedElementsAreArray(keys_expected));
  EXPECT_THAT(values_result,
              testing::UnorderedElementsAreArray(values_expected));
}

TEST_F(CausalCacheTest, CrossCoveredInPreparation) {
  Key key = "a";
  VectorClock vc = construct_vector_clock({{"c1", 1}, {"c2", 1}});
  map<Key, VectorClock> dep;
  dep["b"] = construct_vector_clock({{"c1", 3}, {"c2", 2}});
  dep["c"] = construct_vector_clock({{"c1", 2}, {"c2", 1}});
  unmerged_store[key] = construct_causal_lattice(vc, dep, value);
  in_preparation[key].second[key] = unmerged_store[key];

  key = "b";
  vc = construct_vector_clock({{"c1", 3}, {"c2", 3}});
  dep.clear();
  unmerged_store[key] = construct_causal_lattice(vc, dep, value);
  in_preparation["a"].second[key] = unmerged_store[key];

  // issue GET request
  CausalRequest request;
  request.set_consistency(ConsistencyType::CROSS);
  request.set_id("test");

  CausalTuple* tp = request.add_tuples();
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

  vector<Key> keys_expected = {"b"};
  vector<string> values_expected = {value};

  vector<Key> keys_result;
  vector<string> values_result;

  for (const CausalTuple& tp : response.tuples()) {
    keys_result.push_back(tp.key());
    values_result.push_back(deserialize_cross_causal(tp.payload()).values(0));
  }

  EXPECT_THAT(keys_result, testing::UnorderedElementsAreArray(keys_expected));
  EXPECT_THAT(values_result,
              testing::UnorderedElementsAreArray(values_expected));
}

TEST_F(CausalCacheTest, CrossNotCovered) {
  // issue GET request
  CausalRequest request;
  request.set_consistency(ConsistencyType::CROSS);
  request.set_id("test");

  CausalTuple* tp = request.add_tuples();
  tp->set_key("a");

  request.set_response_address(dummy_address);

  string serialized;
  request.SerializeToString(&serialized);

  get_request_handler(serialized, key_set, unmerged_store, in_preparation,
                      causal_cut_store, version_store, single_callback_map,
                      pending_single_metadata, pending_cross_metadata,
                      to_fetch_map, cover_map, pushers, client, log_, cct,
                      client_id_to_address_map);

  map<Key, set<Key>> expected_to_fetch_map = {{"a", {}}};

  EXPECT_THAT(to_fetch_map,
              testing::UnorderedElementsAreArray(expected_to_fetch_map));
  EXPECT_EQ(mock_cl.keys_get_.size(), 1);
  EXPECT_EQ(*(mock_cl.keys_get_.begin()), "a");

  // generate a mock KeyResponse for key "a" with no dependency
  KeyResponse response;
  response.set_type(RequestType::GET);
  KeyTuple* ktp = response.add_tuples();
  ktp->set_key("a");
  ktp->set_error(0);
  VectorClock vc = construct_vector_clock({{"c1", 1}, {"c2", 1}});
  map<Key, VectorClock> dep;
  ktp->set_payload(serialize(*construct_causal_lattice(vc, dep, value)));

  kvs_response_handler(response, unmerged_store, in_preparation,
                       causal_cut_store, version_store, single_callback_map,
                       pending_single_metadata, pending_cross_metadata,
                       to_fetch_map, cover_map, pushers, client, log_, cct,
                       client_id_to_address_map, request_id_to_address_map);

  EXPECT_EQ(to_fetch_map.size(), 0);

  CausalResponse causal_response;
  causal_response.ParseFromString(mock_zmq_util.sent_messages[0]);

  vector<Key> keys_expected = {"a"};
  vector<string> values_expected = {value};

  vector<Key> keys_result;
  vector<string> values_result;

  for (const CausalTuple& tp : causal_response.tuples()) {
    keys_result.push_back(tp.key());
    values_result.push_back(deserialize_cross_causal(tp.payload()).values(0));
  }

  EXPECT_THAT(keys_result, testing::UnorderedElementsAreArray(keys_expected));
  EXPECT_THAT(values_result,
              testing::UnorderedElementsAreArray(values_expected));
}

TEST_F(CausalCacheTest, CrossDepNotCovered) {
  Key key = "a";
  VectorClock vc = construct_vector_clock({{"c1", 1}, {"c2", 1}});
  map<Key, VectorClock> dep;
  dep["b"] = construct_vector_clock({{"c1", 3}, {"c2", 2}});
  unmerged_store[key] = construct_causal_lattice(vc, dep, value);

  // issue GET request
  CausalRequest request;
  request.set_consistency(ConsistencyType::CROSS);
  request.set_id("test");

  CausalTuple* tp = request.add_tuples();
  tp->set_key("a");

  request.set_response_address(dummy_address);

  string serialized;
  request.SerializeToString(&serialized);

  get_request_handler(serialized, key_set, unmerged_store, in_preparation,
                      causal_cut_store, version_store, single_callback_map,
                      pending_single_metadata, pending_cross_metadata,
                      to_fetch_map, cover_map, pushers, client, log_, cct,
                      client_id_to_address_map);

  map<Key, set<Key>> expected_to_fetch_map = {{"a", {"b"}}};

  EXPECT_THAT(to_fetch_map,
              testing::UnorderedElementsAreArray(expected_to_fetch_map));
  EXPECT_EQ(mock_cl.keys_get_.size(), 1);
  EXPECT_EQ(*(mock_cl.keys_get_.begin()), "b");

  // generate a mock KeyResponse for key "b" with insufficient vector clock
  KeyResponse response;
  response.set_type(RequestType::GET);
  KeyTuple* ktp = response.add_tuples();
  ktp->set_key("b");
  ktp->set_error(0);
  vc = construct_vector_clock({{"c1", 2}, {"c2", 2}});
  dep.clear();
  ktp->set_payload(serialize(*construct_causal_lattice(vc, dep, value)));

  kvs_response_handler(response, unmerged_store, in_preparation,
                       causal_cut_store, version_store, single_callback_map,
                       pending_single_metadata, pending_cross_metadata,
                       to_fetch_map, cover_map, pushers, client, log_, cct,
                       client_id_to_address_map, request_id_to_address_map);

  EXPECT_THAT(to_fetch_map,
              testing::UnorderedElementsAreArray(expected_to_fetch_map));
  EXPECT_EQ(mock_cl.keys_get_.size(), 2);
  EXPECT_THAT(mock_cl.keys_get_,
              testing::UnorderedElementsAreArray({"b", "b"}));

  // generate a mock KeyResponse for key "b" with dependency "c"
  response.Clear();
  response.set_type(RequestType::GET);
  ktp = response.add_tuples();
  ktp->set_key("b");
  ktp->set_error(0);
  vc = construct_vector_clock({{"c1", 3}, {"c2", 4}});
  dep.clear();
  dep["c"] = construct_vector_clock({{"c1", 1}, {"c2", 1}});
  ktp->set_payload(serialize(*construct_causal_lattice(vc, dep, value)));

  kvs_response_handler(response, unmerged_store, in_preparation,
                       causal_cut_store, version_store, single_callback_map,
                       pending_single_metadata, pending_cross_metadata,
                       to_fetch_map, cover_map, pushers, client, log_, cct,
                       client_id_to_address_map, request_id_to_address_map);

  expected_to_fetch_map = {{"a", {"c"}}};

  EXPECT_THAT(to_fetch_map,
              testing::UnorderedElementsAreArray(expected_to_fetch_map));
  EXPECT_EQ(mock_cl.keys_get_.size(), 3);
  EXPECT_THAT(mock_cl.keys_get_,
              testing::UnorderedElementsAreArray({"b", "b", "c"}));

  // generate a mock KeyResponse for key "c"
  response.Clear();
  response.set_type(RequestType::GET);
  ktp = response.add_tuples();
  ktp->set_key("c");
  ktp->set_error(0);
  vc = construct_vector_clock({{"c1", 1}, {"c2", 1}});
  dep.clear();
  ktp->set_payload(serialize(*construct_causal_lattice(vc, dep, value)));

  kvs_response_handler(response, unmerged_store, in_preparation,
                       causal_cut_store, version_store, single_callback_map,
                       pending_single_metadata, pending_cross_metadata,
                       to_fetch_map, cover_map, pushers, client, log_, cct,
                       client_id_to_address_map, request_id_to_address_map);

  EXPECT_EQ(to_fetch_map.size(), 0);

  EXPECT_EQ(mock_zmq_util.sent_messages.size(), 1);

  CausalResponse causal_response;
  causal_response.ParseFromString(mock_zmq_util.sent_messages[0]);

  vector<Key> keys_expected = {"a"};
  vector<string> values_expected = {value};

  vector<Key> keys_result;
  vector<string> values_result;

  for (const CausalTuple& tp : causal_response.tuples()) {
    keys_result.push_back(tp.key());
    values_result.push_back(deserialize_cross_causal(tp.payload()).values(0));
  }

  EXPECT_THAT(keys_result, testing::UnorderedElementsAreArray(keys_expected));
  EXPECT_THAT(values_result,
              testing::UnorderedElementsAreArray(values_expected));
}

TEST_F(CausalCacheTest, CrossQuerySameKey) {
  Key key = "a";
  VectorClock vc = construct_vector_clock({{"c1", 1}, {"c2", 1}});
  map<Key, VectorClock> dep;
  dep["b"] = construct_vector_clock({{"c1", 3}, {"c2", 2}});
  unmerged_store[key] = construct_causal_lattice(vc, dep, value);

  // issue GET request
  CausalRequest request;
  request.set_consistency(ConsistencyType::CROSS);
  request.set_id("test1");
  // add two keys
  CausalTuple* tp = request.add_tuples();
  tp->set_key("a");

  request.set_response_address(dummy_address + "1");

  string serialized;
  request.SerializeToString(&serialized);

  get_request_handler(serialized, key_set, unmerged_store, in_preparation,
                      causal_cut_store, version_store, single_callback_map,
                      pending_single_metadata, pending_cross_metadata,
                      to_fetch_map, cover_map, pushers, client, log_, cct,
                      client_id_to_address_map);

  EXPECT_EQ(mock_cl.keys_get_.size(), 1);
  EXPECT_EQ(*(mock_cl.keys_get_.begin()), "b");

  // issue another GET request
  request.Clear();
  request.set_consistency(ConsistencyType::CROSS);
  request.set_id("test2");
  // add two keys
  tp = request.add_tuples();
  tp->set_key("a");

  request.set_response_address(dummy_address + "2");

  request.SerializeToString(&serialized);

  get_request_handler(serialized, key_set, unmerged_store, in_preparation,
                      causal_cut_store, version_store, single_callback_map,
                      pending_single_metadata, pending_cross_metadata,
                      to_fetch_map, cover_map, pushers, client, log_, cct,
                      client_id_to_address_map);

  EXPECT_EQ(mock_cl.keys_get_.size(), 1);
  EXPECT_EQ(*(mock_cl.keys_get_.begin()), "b");

  map<Address, PendingClientMetadata> expected_pending_cross_metadata = {
      {dummy_address + "1", PendingClientMetadata("test1", {"a"}, {"a"})},
      {dummy_address + "2", PendingClientMetadata("test2", {"a"}, {"a"})}};

  EXPECT_THAT(pending_cross_metadata, testing::UnorderedElementsAreArray(
                                          expected_pending_cross_metadata));
  EXPECT_THAT(in_preparation["a"].first,
              testing::UnorderedElementsAreArray(
                  {dummy_address + "1", dummy_address + "2"}));

  // generate a mock KeyResponse for key "b"
  KeyResponse response;
  response.set_type(RequestType::GET);
  KeyTuple* ktp = response.add_tuples();
  ktp->set_key("b");
  ktp->set_error(0);
  vc = construct_vector_clock({{"c1", 4}, {"c2", 4}});
  dep.clear();
  ktp->set_payload(serialize(*construct_causal_lattice(vc, dep, value)));

  kvs_response_handler(response, unmerged_store, in_preparation,
                       causal_cut_store, version_store, single_callback_map,
                       pending_single_metadata, pending_cross_metadata,
                       to_fetch_map, cover_map, pushers, client, log_, cct,
                       client_id_to_address_map, request_id_to_address_map);

  EXPECT_EQ(mock_zmq_util.sent_messages.size(), 2);
}