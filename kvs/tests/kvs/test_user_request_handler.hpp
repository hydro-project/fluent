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

TEST_F(ServerHandlerTest, UserGetLWWTest) {
  Key key = "key";
  std::string value = "value";
  serializers[LatticeType::LWW]->put(key, serialize(0, value));
  key_stat_map[key].second = LatticeType::LWW;

  std::string get_request = get_key_request(key, LatticeType::LWW, ip);

  unsigned total_access = 0;
  unsigned seed = 0;

  EXPECT_EQ(local_changeset.size(), 0);

  user_request_handler(total_access, seed, get_request, logger,
                       global_hash_ring_map, local_hash_ring_map, key_stat_map,
                       pending_request_map, key_access_timestamp, placement,
                       local_changeset, wt, serializers, pushers);

  std::vector<std::string> messages = get_zmq_messages();
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
  EXPECT_EQ(total_access, 1);
  EXPECT_EQ(key_access_timestamp[key].size(), 1);
}

TEST_F(ServerHandlerTest, UserGetSetTest) {
  Key key = "key";
  std::unordered_set<std::string> s;
  s.emplace("value1");
  s.emplace("value2");
  s.emplace("value3");
  serializers[LatticeType::SET]->put(key,
                                     serialize(SetLattice<std::string>(s)));
  key_stat_map[key].second = LatticeType::SET;

  std::string get_request = get_key_request(key, LatticeType::SET, ip);

  unsigned total_access = 0;
  unsigned seed = 0;

  EXPECT_EQ(local_changeset.size(), 0);

  user_request_handler(total_access, seed, get_request, logger,
                       global_hash_ring_map, local_hash_ring_map, key_stat_map,
                       pending_request_map, key_access_timestamp, placement,
                       local_changeset, wt, serializers, pushers);

  std::vector<std::string> messages = get_zmq_messages();
  EXPECT_EQ(messages.size(), 1);

  KeyResponse response;
  response.ParseFromString(messages[0]);

  EXPECT_EQ(response.response_id(), kRequestId);
  EXPECT_EQ(response.tuples().size(), 1);

  KeyTuple rtp = response.tuples(0);

  EXPECT_EQ(rtp.key(), key);
  EXPECT_EQ(rtp.payload(), serialize(SetLattice<std::string>(s)));
  EXPECT_EQ(rtp.error(), 0);

  EXPECT_EQ(local_changeset.size(), 0);
  EXPECT_EQ(total_access, 1);
  EXPECT_EQ(key_access_timestamp[key].size(), 1);
}

TEST_F(ServerHandlerTest, UserPutAndGetLWWTest) {
  Key key = "key";
  std::string value = "value";
  std::string put_request =
      put_key_request(key, LatticeType::LWW, serialize(0, value), ip);

  unsigned total_access = 0;
  unsigned seed = 0;

  EXPECT_EQ(local_changeset.size(), 0);

  user_request_handler(total_access, seed, put_request, logger,
                       global_hash_ring_map, local_hash_ring_map, key_stat_map,
                       pending_request_map, key_access_timestamp, placement,
                       local_changeset, wt, serializers, pushers);

  std::vector<std::string> messages = get_zmq_messages();
  EXPECT_EQ(messages.size(), 1);

  KeyResponse response;
  response.ParseFromString(messages[0]);

  EXPECT_EQ(response.response_id(), kRequestId);
  EXPECT_EQ(response.tuples().size(), 1);

  KeyTuple rtp = response.tuples(0);

  EXPECT_EQ(rtp.key(), key);
  EXPECT_EQ(rtp.error(), 0);

  EXPECT_EQ(local_changeset.size(), 1);
  EXPECT_EQ(total_access, 1);
  EXPECT_EQ(key_access_timestamp[key].size(), 1);

  std::string get_request = get_key_request(key, LatticeType::LWW, ip);

  user_request_handler(total_access, seed, get_request, logger,
                       global_hash_ring_map, local_hash_ring_map, key_stat_map,
                       pending_request_map, key_access_timestamp, placement,
                       local_changeset, wt, serializers, pushers);

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
  EXPECT_EQ(total_access, 2);
  EXPECT_EQ(key_access_timestamp[key].size(), 2);
}

TEST_F(ServerHandlerTest, UserPutAndGetSetTest) {
  Key key = "key";
  std::unordered_set<std::string> s;
  s.emplace("value1");
  s.emplace("value2");
  s.emplace("value3");
  std::string put_request = put_key_request(
      key, LatticeType::SET, serialize(SetLattice<std::string>(s)), ip);

  unsigned total_access = 0;
  unsigned seed = 0;

  EXPECT_EQ(local_changeset.size(), 0);

  user_request_handler(total_access, seed, put_request, logger,
                       global_hash_ring_map, local_hash_ring_map, key_stat_map,
                       pending_request_map, key_access_timestamp, placement,
                       local_changeset, wt, serializers, pushers);

  std::vector<std::string> messages = get_zmq_messages();
  EXPECT_EQ(messages.size(), 1);

  KeyResponse response;
  response.ParseFromString(messages[0]);

  EXPECT_EQ(response.response_id(), kRequestId);
  EXPECT_EQ(response.tuples().size(), 1);

  KeyTuple rtp = response.tuples(0);

  EXPECT_EQ(rtp.key(), key);
  EXPECT_EQ(rtp.error(), 0);

  EXPECT_EQ(local_changeset.size(), 1);
  EXPECT_EQ(total_access, 1);
  EXPECT_EQ(key_access_timestamp[key].size(), 1);

  std::string get_request = get_key_request(key, LatticeType::SET, ip);

  user_request_handler(total_access, seed, get_request, logger,
                       global_hash_ring_map, local_hash_ring_map, key_stat_map,
                       pending_request_map, key_access_timestamp, placement,
                       local_changeset, wt, serializers, pushers);

  messages = get_zmq_messages();
  EXPECT_EQ(messages.size(), 2);

  response.ParseFromString(messages[1]);

  EXPECT_EQ(response.response_id(), kRequestId);
  EXPECT_EQ(response.tuples().size(), 1);

  rtp = response.tuples(0);

  EXPECT_EQ(rtp.key(), key);
  EXPECT_EQ(rtp.payload(), serialize(SetLattice<std::string>(s)));
  EXPECT_EQ(rtp.error(), 0);

  EXPECT_EQ(local_changeset.size(), 1);
  EXPECT_EQ(total_access, 2);
  EXPECT_EQ(key_access_timestamp[key].size(), 2);
}

// TODO: Test key address cache invalidation
// TODO: Test replication factor request and making the request pending
// TODO: Test metadata operations -- does this matter?
