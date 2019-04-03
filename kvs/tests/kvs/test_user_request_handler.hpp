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

AdaptiveThresholdHeavyHitters* sketch = new AdaptiveThresholdHeavyHitters();

TEST_F(ServerHandlerTest, UserGetLWWTest) {
  Key key = "key";
  string value = "value";
  serializers[LatticeType::LWW]->put(key, serialize(0, value));
  stored_key_map[key].type_ = LatticeType::LWW;

  string get_request = get_key_request(key, ip);

  unsigned access_count = 0;
  unsigned seed = 0;

  EXPECT_EQ(local_changeset.size(), 0);

  user_request_handler(access_count, seed, get_request, log_, global_hash_rings,
                       local_hash_rings, pending_requests, key_access_tracker,
                       stored_key_map, key_replication_map, local_changeset, wt,
                       serializers, pushers, sketch);

  vector<string> messages = get_zmq_messages();
  EXPECT_EQ(messages.size(), 1);

  KeyResponse response;
  response.ParseFromString(messages[0]);

  EXPECT_EQ(response.response_id(), kRequestId);
  EXPECT_EQ(response.tuples().size(), 1);

  KeyTuple rtp = response.tuples(0);

  EXPECT_EQ(rtp.key(), key);
  EXPECT_EQ(rtp.payload(), serialize(0, value));
  EXPECT_EQ(rtp.error(), 0);

  EXPECT_EQ(local_changeset.size(), 0);
  EXPECT_EQ(access_count, 1);
  EXPECT_EQ(key_access_tracker[key].size(), 1);
}

TEST_F(ServerHandlerTest, UserGetSetTest) {
  Key key = "key";
  set<string> s;
  s.emplace("value1");
  s.emplace("value2");
  s.emplace("value3");
  serializers[LatticeType::SET]->put(key, serialize(SetLattice<string>(s)));
  stored_key_map[key].type_ = LatticeType::SET;

  string get_request = get_key_request(key, ip);

  unsigned access_count = 0;
  unsigned seed = 0;

  EXPECT_EQ(local_changeset.size(), 0);

  user_request_handler(access_count, seed, get_request, log_, global_hash_rings,
                       local_hash_rings, pending_requests, key_access_tracker,
                       stored_key_map, key_replication_map, local_changeset, wt,
                       serializers, pushers, sketch);

  vector<string> messages = get_zmq_messages();
  EXPECT_EQ(messages.size(), 1);

  KeyResponse response;
  response.ParseFromString(messages[0]);

  EXPECT_EQ(response.response_id(), kRequestId);
  EXPECT_EQ(response.tuples().size(), 1);

  KeyTuple rtp = response.tuples(0);

  EXPECT_EQ(rtp.key(), key);
  EXPECT_EQ(rtp.payload(), serialize(SetLattice<string>(s)));
  EXPECT_EQ(rtp.error(), 0);

  EXPECT_EQ(local_changeset.size(), 0);
  EXPECT_EQ(access_count, 1);
  EXPECT_EQ(key_access_tracker[key].size(), 1);
}

TEST_F(ServerHandlerTest, UserGetOrderedSetTest) {
  Key key = "key";
  ordered_set<string> s;
  s.emplace("value1");
  s.emplace("value2");
  s.emplace("value3");
  serializers[LatticeType::ORDERED_SET]->put(
      key, serialize(OrderedSetLattice<string>(s)));
  stored_key_map[key].type_ = LatticeType::ORDERED_SET;

  string get_request = get_key_request(key, ip);

  unsigned access_count = 0;
  unsigned seed = 0;

  EXPECT_EQ(local_changeset.size(), 0);

  user_request_handler(access_count, seed, get_request, log_, global_hash_rings,
                       local_hash_rings, pending_requests, key_access_tracker,
                       stored_key_map, key_replication_map, local_changeset, wt,
                       serializers, pushers);

  vector<string> messages = get_zmq_messages();
  EXPECT_EQ(messages.size(), 1);

  KeyResponse response;
  response.ParseFromString(messages[0]);

  EXPECT_EQ(response.response_id(), kRequestId);
  EXPECT_EQ(response.tuples().size(), 1);

  KeyTuple rtp = response.tuples(0);

  EXPECT_EQ(rtp.key(), key);
  EXPECT_EQ(rtp.payload(), serialize(OrderedSetLattice<string>(s)));
  EXPECT_EQ(rtp.error(), 0);

  EXPECT_EQ(local_changeset.size(), 0);
  EXPECT_EQ(access_count, 1);
  EXPECT_EQ(key_access_tracker[key].size(), 1);
}

TEST_F(ServerHandlerTest, UserGetCausalTest) {
  Key key = "key";
  VectorClockValuePair<SetLattice<string>> p;
  p.vector_clock.insert("1", 1);
  p.vector_clock.insert("2", 1);
  p.value.insert("value1");
  p.value.insert("value2");
  p.value.insert("value3");

  serializers[LatticeType::CAUSAL]->put(
      key, serialize(CausalPairLattice<SetLattice<string>>(p)));
  stored_key_map[key].type_ = LatticeType::CAUSAL;

  string get_request = get_key_request(key, ip);

  unsigned access_count = 0;
  unsigned seed = 0;

  EXPECT_EQ(local_changeset.size(), 0);

  user_request_handler(access_count, seed, get_request, log_, global_hash_rings,
                       local_hash_rings, pending_requests, key_access_tracker,
                       stored_key_map, key_replication_map, local_changeset, wt,
                       serializers, pushers, sketch);

  vector<string> messages = get_zmq_messages();
  EXPECT_EQ(messages.size(), 1);

  KeyResponse response;
  response.ParseFromString(messages[0]);

  EXPECT_EQ(response.response_id(), kRequestId);
  EXPECT_EQ(response.tuples().size(), 1);

  KeyTuple rtp = response.tuples(0);

  EXPECT_EQ(rtp.key(), key);

  CausalValue left_value;
  CausalValue right_value;
  left_value.ParseFromString(rtp.payload());
  right_value.ParseFromString(
      serialize(CausalPairLattice<SetLattice<string>>(p)));

  set<string> left_set;
  set<string> right_set;

  for (const auto& val : left_value.values()) {
    left_set.insert(val);
  }
  for (const auto& val : right_value.values()) {
    right_set.insert(val);
  }

  EXPECT_THAT(left_set, testing::UnorderedElementsAreArray(right_set));

  map<string, unsigned> left_map;
  map<string, unsigned> right_map;

  for (const auto& pair : left_value.vector_clock()) {
    left_map[pair.first] = pair.second;
  }
  for (const auto& pair : right_value.vector_clock()) {
    right_map[pair.first] = pair.second;
  }

  EXPECT_THAT(left_map, testing::UnorderedElementsAreArray(right_map));

  EXPECT_EQ(rtp.error(), 0);

  EXPECT_EQ(local_changeset.size(), 0);
  EXPECT_EQ(access_count, 1);
  EXPECT_EQ(key_access_tracker[key].size(), 1);
}

TEST_F(ServerHandlerTest, UserPutAndGetLWWTest) {
  Key key = "key";
  string value = "value";
  string put_request =
      put_key_request(key, LatticeType::LWW, serialize(0, value), ip);

  unsigned access_count = 0;
  unsigned seed = 0;

  EXPECT_EQ(local_changeset.size(), 0);

  user_request_handler(access_count, seed, put_request, log_, global_hash_rings,
                       local_hash_rings, pending_requests, key_access_tracker,
                       stored_key_map, key_replication_map, local_changeset, wt,
                       serializers, pushers, sketch);

  vector<string> messages = get_zmq_messages();
  EXPECT_EQ(messages.size(), 1);

  KeyResponse response;
  response.ParseFromString(messages[0]);

  EXPECT_EQ(response.response_id(), kRequestId);
  EXPECT_EQ(response.tuples().size(), 1);

  KeyTuple rtp = response.tuples(0);

  EXPECT_EQ(rtp.key(), key);
  EXPECT_EQ(rtp.error(), 0);

  EXPECT_EQ(local_changeset.size(), 1);
  EXPECT_EQ(access_count, 1);
  EXPECT_EQ(key_access_tracker[key].size(), 1);

  string get_request = get_key_request(key, ip);

  user_request_handler(access_count, seed, get_request, log_, global_hash_rings,
                       local_hash_rings, pending_requests, key_access_tracker,
                       stored_key_map, key_replication_map, local_changeset, wt,
                       serializers, pushers, sketch);

  messages = get_zmq_messages();
  EXPECT_EQ(messages.size(), 2);

  response.ParseFromString(messages[1]);

  EXPECT_EQ(response.response_id(), kRequestId);
  EXPECT_EQ(response.tuples().size(), 1);

  rtp = response.tuples(0);

  EXPECT_EQ(rtp.key(), key);
  EXPECT_EQ(rtp.payload(), serialize(0, value));
  EXPECT_EQ(rtp.error(), 0);

  EXPECT_EQ(local_changeset.size(), 1);
  EXPECT_EQ(access_count, 2);
  EXPECT_EQ(key_access_tracker[key].size(), 2);
}

TEST_F(ServerHandlerTest, UserPutAndGetSetTest) {
  Key key = "key";
  set<string> s;
  s.emplace("value1");
  s.emplace("value2");
  s.emplace("value3");
  string put_request = put_key_request(key, LatticeType::SET,
                                       serialize(SetLattice<string>(s)), ip);

  unsigned access_count = 0;
  unsigned seed = 0;

  EXPECT_EQ(local_changeset.size(), 0);

  user_request_handler(access_count, seed, put_request, log_, global_hash_rings,
                       local_hash_rings, pending_requests, key_access_tracker,
                       stored_key_map, key_replication_map, local_changeset, wt,
                       serializers, pushers, sketch);

  vector<string> messages = get_zmq_messages();
  EXPECT_EQ(messages.size(), 1);

  KeyResponse response;
  response.ParseFromString(messages[0]);

  EXPECT_EQ(response.response_id(), kRequestId);
  EXPECT_EQ(response.tuples().size(), 1);

  KeyTuple rtp = response.tuples(0);

  EXPECT_EQ(rtp.key(), key);
  EXPECT_EQ(rtp.error(), 0);

  EXPECT_EQ(local_changeset.size(), 1);
  EXPECT_EQ(access_count, 1);
  EXPECT_EQ(key_access_tracker[key].size(), 1);

  string get_request = get_key_request(key, ip);

  user_request_handler(access_count, seed, get_request, log_, global_hash_rings,
                       local_hash_rings, pending_requests, key_access_tracker,
                       stored_key_map, key_replication_map, local_changeset, wt,
                       serializers, pushers, sketch);

  messages = get_zmq_messages();
  EXPECT_EQ(messages.size(), 2);

  response.ParseFromString(messages[1]);

  EXPECT_EQ(response.response_id(), kRequestId);
  EXPECT_EQ(response.tuples().size(), 1);

  rtp = response.tuples(0);

  EXPECT_EQ(rtp.key(), key);
  EXPECT_EQ(rtp.payload(), serialize(SetLattice<string>(s)));
  EXPECT_EQ(rtp.error(), 0);

  EXPECT_EQ(local_changeset.size(), 1);
  EXPECT_EQ(access_count, 2);
  EXPECT_EQ(key_access_tracker[key].size(), 2);
}

TEST_F(ServerHandlerTest, UserPutAndGetOrderedSetTest) {
  Key key = "key";
  ordered_set<string> s;
  s.emplace("value1");
  s.emplace("value2");
  s.emplace("value3");
  string put_request = put_key_request(
      key, LatticeType::SET, serialize(OrderedSetLattice<string>(s)), ip);

  unsigned access_count = 0;
  unsigned seed = 0;

  EXPECT_EQ(local_changeset.size(), 0);

  user_request_handler(access_count, seed, put_request, log_, global_hash_rings,
                       local_hash_rings, pending_requests, key_access_tracker,
                       stored_key_map, key_replication_map, local_changeset, wt,
                       serializers, pushers);

  vector<string> messages = get_zmq_messages();
  EXPECT_EQ(messages.size(), 1);

  KeyResponse response;
  response.ParseFromString(messages[0]);

  EXPECT_EQ(response.response_id(), kRequestId);
  EXPECT_EQ(response.tuples().size(), 1);

  KeyTuple rtp = response.tuples(0);

  EXPECT_EQ(rtp.key(), key);
  EXPECT_EQ(rtp.error(), 0);

  EXPECT_EQ(local_changeset.size(), 1);
  EXPECT_EQ(access_count, 1);
  EXPECT_EQ(key_access_tracker[key].size(), 1);

  string get_request = get_key_request(key, ip);

  user_request_handler(access_count, seed, get_request, log_, global_hash_rings,
                       local_hash_rings, pending_requests, key_access_tracker,
                       stored_key_map, key_replication_map, local_changeset, wt,
                       serializers, pushers);

  messages = get_zmq_messages();
  EXPECT_EQ(messages.size(), 2);

  response.ParseFromString(messages[1]);

  EXPECT_EQ(response.response_id(), kRequestId);
  EXPECT_EQ(response.tuples().size(), 1);

  rtp = response.tuples(0);

  EXPECT_EQ(rtp.key(), key);
  EXPECT_EQ(rtp.payload(), serialize(OrderedSetLattice<string>(s)));
  EXPECT_EQ(rtp.error(), 0);

  EXPECT_EQ(local_changeset.size(), 1);
  EXPECT_EQ(access_count, 2);
  EXPECT_EQ(key_access_tracker[key].size(), 2);
}

TEST_F(ServerHandlerTest, UserPutAndGetCausalTest) {
  Key key = "key";
  VectorClockValuePair<SetLattice<string>> p;
  p.vector_clock.insert("1", 1);
  p.vector_clock.insert("2", 1);
  p.value.insert("value1");
  p.value.insert("value2");
  p.value.insert("value3");
  string put_request =
      put_key_request(key, LatticeType::CAUSAL,
                      serialize(CausalPairLattice<SetLattice<string>>(p)), ip);

  unsigned access_count = 0;
  unsigned seed = 0;

  EXPECT_EQ(local_changeset.size(), 0);

  user_request_handler(access_count, seed, put_request, log_, global_hash_rings,
                       local_hash_rings, pending_requests, key_access_tracker,
                       stored_key_map, key_replication_map, local_changeset, wt,
                       serializers, pushers, sketch);

  vector<string> messages = get_zmq_messages();
  EXPECT_EQ(messages.size(), 1);

  KeyResponse response;
  response.ParseFromString(messages[0]);

  EXPECT_EQ(response.response_id(), kRequestId);
  EXPECT_EQ(response.tuples().size(), 1);

  KeyTuple rtp = response.tuples(0);

  EXPECT_EQ(rtp.key(), key);
  EXPECT_EQ(rtp.error(), 0);

  EXPECT_EQ(local_changeset.size(), 1);
  EXPECT_EQ(access_count, 1);
  EXPECT_EQ(key_access_tracker[key].size(), 1);

  string get_request = get_key_request(key, ip);

  user_request_handler(access_count, seed, get_request, log_, global_hash_rings,
                       local_hash_rings, pending_requests, key_access_tracker,
                       stored_key_map, key_replication_map, local_changeset, wt,
                       serializers, pushers, sketch);

  messages = get_zmq_messages();
  EXPECT_EQ(messages.size(), 2);

  response.ParseFromString(messages[1]);

  EXPECT_EQ(response.response_id(), kRequestId);
  EXPECT_EQ(response.tuples().size(), 1);

  rtp = response.tuples(0);

  EXPECT_EQ(rtp.key(), key);

  CausalValue left_value;
  CausalValue right_value;
  left_value.ParseFromString(rtp.payload());
  right_value.ParseFromString(
      serialize(CausalPairLattice<SetLattice<string>>(p)));

  set<string> left_set;
  set<string> right_set;

  for (const auto& val : left_value.values()) {
    left_set.insert(val);
  }
  for (const auto& val : right_value.values()) {
    right_set.insert(val);
  }

  EXPECT_THAT(left_set, testing::UnorderedElementsAreArray(right_set));

  map<string, unsigned> left_map;
  map<string, unsigned> right_map;

  for (const auto& pair : left_value.vector_clock()) {
    left_map[pair.first] = pair.second;
  }
  for (const auto& pair : right_value.vector_clock()) {
    right_map[pair.first] = pair.second;
  }

  EXPECT_THAT(left_map, testing::UnorderedElementsAreArray(right_map));

  EXPECT_EQ(rtp.error(), 0);

  EXPECT_EQ(local_changeset.size(), 1);
  EXPECT_EQ(access_count, 2);
  EXPECT_EQ(key_access_tracker[key].size(), 2);
}

// TODO: Test key address cache invalidation
// TODO: Test replication factor request and making the request pending
// TODO: Test metadata operations -- does this matter?
