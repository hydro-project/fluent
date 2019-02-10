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

#ifndef SRC_INCLUDE_UTILS_SERVER_UTILS_HPP_
#define SRC_INCLUDE_UTILS_SERVER_UTILS_HPP_

#include <fstream>
#include <string>

#include "common.hpp"
#include "../kvs/base_kv_store.hpp"
#include "../kvs/lww_pair_lattice.hpp"
#include "yaml-cpp/yaml.h"

// Define the garbage collect threshold
#define GARBAGE_COLLECT_THRESHOLD 10000000

// Define the data redistribute threshold
#define DATA_REDISTRIBUTE_THRESHOLD 50

// Define the gossip period (frequency)
#define PERIOD 10000000

typedef KVStore<Key, LWWPairLattice<std::string>> MemoryLWWKVS;
typedef KVStore<Key, SetLattice<std::string>> MemorySetKVS;

// a map that represents which keys should be sent to which IP-port combinations
typedef std::unordered_map<Address, std::unordered_set<Key>> AddressKeysetMap;

class Serializer {
 public:
  virtual std::string get(const Key& key, unsigned& err_number) = 0;
  virtual bool put(const Key& key, const std::string& serialized) = 0;
  virtual void remove(const Key& key) = 0;
  virtual ~Serializer(){};
};

class MemoryLWWSerializer : public Serializer {
  MemoryLWWKVS* kvs_;

 public:
  MemoryLWWSerializer(MemoryLWWKVS* kvs) : kvs_(kvs) {}

  std::string get(const Key& key, unsigned& err_number) {
    auto val = kvs_->get(key, err_number);
    if (val.reveal().value == "") {
      err_number = 1;
    }
    return serialize(val);
  }

  bool put(const Key& key, const std::string& serialized) {
    LWWValue lww_value;
    lww_value.ParseFromString(serialized);
    TimestampValuePair<std::string> p =
        TimestampValuePair<std::string>(lww_value.timestamp(), lww_value.value());
    return kvs_->put(key, LWWPairLattice<std::string>(p));
  }

  void remove(const Key& key) { kvs_->remove(key); }
};

class EBSLWWSerializer : public Serializer {
  unsigned tid_;
  std::string ebs_root_;

 public:
  EBSLWWSerializer(unsigned& tid) : tid_(tid) {
    YAML::Node conf = YAML::LoadFile("conf/kvs-config.yml");

    ebs_root_ = conf["ebs"].as<std::string>();

    if (ebs_root_.back() != '/') {
      ebs_root_ += "/";
    }
  }

  std::string get(const Key& key,
                                            unsigned& err_number) {
    std::string res;
    DataValue value;

    // open a new filestream for reading in a binary
    std::string fname = ebs_root_ + "ebs_" + std::to_string(tid_) + "/" + key;
    std::fstream input(fname, std::ios::in | std::ios::binary);

    if (!input) {
      err_number = 1;
    } else if (!value.ParseFromIstream(&input)) {
      std::cerr << "Failed to parse payload." << std::endl;
      err_number = 1;
    } else {
      res = serialize(LWWPairLattice<std::string>(
          TimestampValuePair<std::string>(value.timestamp(), value.value())));
    }
    return res;
  }

  bool put(const Key& key, const std::string& serialized) {
    bool replaced = false;
    LWWValue lww_value;
    lww_value.ParseFromString(serialized);
    TimestampValuePair<std::string> p =
        TimestampValuePair<std::string>(lww_value.timestamp(), lww_value.value());

    DataValue original_value;
    DataValue new_value;

    std::string fname = ebs_root_ + "ebs_" + std::to_string(tid_) + "/" + key;
    std::fstream input(fname, std::ios::in | std::ios::binary);

    if (!input) {  // in this case, this key has never been seen before, so we
                   // attempt to create a new file for it
      replaced = true;
      new_value.set_timestamp(lww_value.timestamp());
      new_value.set_value(lww_value.value());

      // ios::trunc means that we overwrite the existing file
      std::fstream output(fname,
                          std::ios::out | std::ios::trunc | std::ios::binary);
      if (!new_value.SerializeToOstream(&output)) {
        std::cerr << "Failed to write payload." << std::endl;
      }
    } else if (!original_value.ParseFromIstream(
                   &input)) {  // if we have seen the key before, attempt to
                               // parse what was there before
      std::cerr << "Failed to parse payload." << std::endl;
    } else {
      // get the existing value that we have and merge
      LWWPairLattice<std::string> l =
          LWWPairLattice<std::string>(TimestampValuePair<std::string>(
              original_value.timestamp(), original_value.value()));
      replaced = l.merge(p);

      if (replaced) {
        // set the payload's data to the merged values of the value and
        // timestamp
        new_value.set_timestamp(l.reveal().timestamp);
        new_value.set_value(l.reveal().value);

        // write out the new payload.
        std::fstream output(fname,
                            std::ios::out | std::ios::trunc | std::ios::binary);

        if (!new_value.SerializeToOstream(&output)) {
          std::cerr << "Failed to write payload" << std::endl;
        }
      }
    }

    return replaced;
  }

  void remove(const Key& key) {
    std::string fname = ebs_root_ + "ebs_" + std::to_string(tid_) + "/" + key;

    if (std::remove(fname.c_str()) != 0) {
      std::cerr << "Error deleting file" << std::endl;
    }
  }
};

struct PendingRequest {
  PendingRequest() {}
  PendingRequest(std::string type, const std::string& value, Address addr,
                 std::string respond_id) :
      type_(type),
      value_(value),
      addr_(addr),
      respond_id_(respond_id) {}

  // TODO(vikram): change these type names
  std::string type_;
  std::string value_;
  Address addr_;
  std::string respond_id_;
};

struct PendingGossip {
  PendingGossip() {}
  PendingGossip(const std::string& payload) :
      payload_(payload){}
  std::string payload_;
};

#endif  // SRC_INCLUDE_UTILS_SERVER_UTILS_HPP_
