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

#include "../kvs/base_kv_store.hpp"
#include "../kvs/lww_pair_lattice.hpp"
#include "common.hpp"
#include "yaml-cpp/yaml.h"

// Define the garbage collect threshold
#define GARBAGE_COLLECT_THRESHOLD 10000000

// Define the data redistribute threshold
#define DATA_REDISTRIBUTE_THRESHOLD 50

// Define the gossip period (frequency)
#define PERIOD 10000000  // 10 seconds

typedef KVStore<Key, LWWPairLattice<string>> MemoryLWWKVS;
typedef KVStore<Key, SetLattice<string>> MemorySetKVS;
typedef KVStore<Key, CausalPairLattice<SetLattice<string>>> MemoryCausalKVS;

// a map that represents which keys should be sent to which IP-port combinations
typedef map<Address, set<Key>> AddressKeysetMap;

class Serializer {
 public:
  virtual string get(const Key& key, unsigned& err_number) = 0;
  virtual unsigned put(const Key& key, const string& serialized) = 0;
  virtual void remove(const Key& key) = 0;
  virtual ~Serializer(){};
};

class MemoryLWWSerializer : public Serializer {
  MemoryLWWKVS* kvs_;

 public:
  MemoryLWWSerializer(MemoryLWWKVS* kvs) : kvs_(kvs) {}

  string get(const Key& key, unsigned& err_number) {
    auto val = kvs_->get(key, err_number);
    if (val.reveal().value == "") {
      err_number = 1;
    }
    return serialize(val);
  }

  unsigned put(const Key& key, const string& serialized) {
    LWWValue lww_value = deserialize_lww(serialized);
    TimestampValuePair<string> p =
        TimestampValuePair<string>(lww_value.timestamp(), lww_value.value());
    kvs_->put(key, LWWPairLattice<string>(p));
    return kvs_->size(key);
  }

  void remove(const Key& key) { kvs_->remove(key); }
};

class MemorySetSerializer : public Serializer {
  MemorySetKVS* kvs_;

 public:
  MemorySetSerializer(MemorySetKVS* kvs) : kvs_(kvs) {}

  string get(const Key& key, unsigned& err_number) {
    auto val = kvs_->get(key, err_number);
    if (val.size().reveal() == 0) {
      err_number = 1;
    }
    return serialize(val);
  }

  unsigned put(const Key& key, const string& serialized) {
    SetValue set_value = deserialize_set(serialized);
    set<string> s;
    for (auto& val : set_value.values()) {
      s.emplace(std::move(val));
    }
    kvs_->put(key, SetLattice<string>(s));
    return kvs_->size(key);
  }

  void remove(const Key& key) { kvs_->remove(key); }
};

class MemoryCausalSerializer : public Serializer {
  MemoryCausalKVS* kvs_;

 public:
  MemoryCausalSerializer(MemoryCausalKVS* kvs) : kvs_(kvs) {}

  string get(const Key& key, unsigned& err_number) {
    auto val = kvs_->get(key, err_number);
    if (val.reveal().value.size().reveal() == 0) {
      err_number = 1;
    }
    return serialize(val);
  }

  unsigned put(const Key& key, const string& serialized) {
    CausalValue causal_value = deserialize_causal(serialized);
    VectorClockValuePair<SetLattice<string>> p;

    for (const auto& pair : causal_value.vector_clock()) {
      p.vector_clock.insert(pair.first, pair.second);
    }
    for (auto& val : causal_value.values()) {
      p.value.insert(std::move(val));
    }
    kvs_->put(key, CausalPairLattice<SetLattice<string>>(p));
    return kvs_->size(key);
  }

  void remove(const Key& key) { kvs_->remove(key); }
};

class EBSLWWSerializer : public Serializer {
  unsigned tid_;
  string ebs_root_;

 public:
  EBSLWWSerializer(unsigned& tid) : tid_(tid) {
    YAML::Node conf = YAML::LoadFile("conf/kvs-config.yml");

    ebs_root_ = conf["ebs"].as<string>();

    if (ebs_root_.back() != '/') {
      ebs_root_ += "/";
    }
  }

  string get(const Key& key, unsigned& err_number) {
    string res;
    LWWValue value;

    // open a new filestream for reading in a binary
    string fname = ebs_root_ + "ebs_" + std::to_string(tid_) + "/" + key;
    std::fstream input(fname, std::ios::in | std::ios::binary);

    if (!input) {
      err_number = 1;
    } else if (!value.ParseFromIstream(&input)) {
      std::cerr << "Failed to parse payload." << std::endl;
      err_number = 1;
    } else {
      if (value.value() == "") {
        err_number = 1;
      } else {
        value.SerializeToString(&res);
      }
    }
    return res;
  }

  unsigned put(const Key& key, const string& serialized) {
    LWWValue input_value;
    input_value.ParseFromString(serialized);

    LWWValue original_value;

    string fname = ebs_root_ + "ebs_" + std::to_string(tid_) + "/" + key;
    std::fstream input(fname, std::ios::in | std::ios::binary);

    if (!input) {  // in this case, this key has never been seen before, so we
                   // attempt to create a new file for it

      // ios::trunc means that we overwrite the existing file
      std::fstream output(fname,
                          std::ios::out | std::ios::trunc | std::ios::binary);
      if (!input_value.SerializeToOstream(&output)) {
        std::cerr << "Failed to write payload." << std::endl;
      }
      return output.tellp();
    } else if (!original_value.ParseFromIstream(
                   &input)) {  // if we have seen the key before, attempt to
                               // parse what was there before
      std::cerr << "Failed to parse payload." << std::endl;
      return 0;
    } else {
      std::fstream output(fname,
                          std::ios::out | std::ios::trunc | std::ios::binary);
      if (input_value.timestamp() >= original_value.timestamp()) {
        if (!input_value.SerializeToOstream(&output)) {
          std::cerr << "Failed to write payload" << std::endl;
        }
      }
      return output.tellp();
    }
  }

  void remove(const Key& key) {
    string fname = ebs_root_ + "ebs_" + std::to_string(tid_) + "/" + key;

    if (std::remove(fname.c_str()) != 0) {
      std::cerr << "Error deleting file" << std::endl;
    }
  }
};

class EBSSetSerializer : public Serializer {
  unsigned tid_;
  string ebs_root_;

 public:
  EBSSetSerializer(unsigned& tid) : tid_(tid) {
    YAML::Node conf = YAML::LoadFile("conf/kvs-config.yml");

    ebs_root_ = conf["ebs"].as<string>();

    if (ebs_root_.back() != '/') {
      ebs_root_ += "/";
    }
  }

  string get(const Key& key, unsigned& err_number) {
    string res;
    SetValue value;

    // open a new filestream for reading in a binary
    string fname = ebs_root_ + "ebs_" + std::to_string(tid_) + "/" + key;
    std::fstream input(fname, std::ios::in | std::ios::binary);

    if (!input) {
      err_number = 1;
    } else if (!value.ParseFromIstream(&input)) {
      std::cerr << "Failed to parse payload." << std::endl;
      err_number = 1;
    } else {
      if (value.values_size() == 0) {
        err_number = 1;
      } else {
        value.SerializeToString(&res);
      }
    }
    return res;
  }

  unsigned put(const Key& key, const string& serialized) {
    SetValue input_value;
    input_value.ParseFromString(serialized);

    SetValue original_value;

    string fname = ebs_root_ + "ebs_" + std::to_string(tid_) + "/" + key;
    std::fstream input(fname, std::ios::in | std::ios::binary);

    if (!input) {  // in this case, this key has never been seen before, so we
                   // attempt to create a new file for it
      // ios::trunc means that we overwrite the existing file
      std::fstream output(fname,
                          std::ios::out | std::ios::trunc | std::ios::binary);
      if (!input_value.SerializeToOstream(&output)) {
        std::cerr << "Failed to write payload." << std::endl;
      }
      return output.tellp();
    } else if (!original_value.ParseFromIstream(
                   &input)) {  // if we have seen the key before, attempt to
                               // parse what was there before
      std::cerr << "Failed to parse payload." << std::endl;
      return 0;
    } else {
      // get the existing value that we have and merge
      set<string> set_union;
      for (auto& val : original_value.values()) {
        set_union.emplace(std::move(val));
      }
      for (auto& val : input_value.values()) {
        set_union.emplace(std::move(val));
      }

      SetValue new_value;
      for (auto& val : set_union) {
        new_value.add_values(std::move(val));
      }

      // write out the new payload.
      std::fstream output(fname,
                          std::ios::out | std::ios::trunc | std::ios::binary);

      if (!new_value.SerializeToOstream(&output)) {
        std::cerr << "Failed to write payload" << std::endl;
      }
      return output.tellp();
    }
  }

  void remove(const Key& key) {
    string fname = ebs_root_ + "ebs_" + std::to_string(tid_) + "/" + key;

    if (std::remove(fname.c_str()) != 0) {
      std::cerr << "Error deleting file" << std::endl;
    }
  }
};

class EBSCausalSerializer : public Serializer {
  unsigned tid_;
  string ebs_root_;

 public:
  EBSCausalSerializer(unsigned& tid) : tid_(tid) {
    YAML::Node conf = YAML::LoadFile("conf/kvs-config.yml");

    ebs_root_ = conf["ebs"].as<string>();

    if (ebs_root_.back() != '/') {
      ebs_root_ += "/";
    }
  }

  string get(const Key& key, unsigned& err_number) {
    string res;
    CausalValue value;

    // open a new filestream for reading in a binary
    string fname = ebs_root_ + "ebs_" + std::to_string(tid_) + "/" + key;
    std::fstream input(fname, std::ios::in | std::ios::binary);

    if (!input) {
      err_number = 1;
    } else if (!value.ParseFromIstream(&input)) {
      std::cerr << "Failed to parse payload." << std::endl;
      err_number = 1;
    } else {
      if (value.values_size() == 0) {
        err_number = 1;
      } else {
        value.SerializeToString(&res);
      }
    }
    return res;
  }

  unsigned put(const Key& key, const string& serialized) {
    CausalValue input_value;
    input_value.ParseFromString(serialized);

    CausalValue original_value;

    string fname = ebs_root_ + "ebs_" + std::to_string(tid_) + "/" + key;
    std::fstream input(fname, std::ios::in | std::ios::binary);

    if (!input) {  // in this case, this key has never been seen before, so we
                   // attempt to create a new file for it
      // ios::trunc means that we overwrite the existing file
      std::fstream output(fname,
                          std::ios::out | std::ios::trunc | std::ios::binary);
      if (!input_value.SerializeToOstream(&output)) {
        std::cerr << "Failed to write payload." << std::endl;
      }
      return output.tellp();
    } else if (!original_value.ParseFromIstream(
                   &input)) {  // if we have seen the key before, attempt to
                               // parse what was there before
      std::cerr << "Failed to parse payload." << std::endl;
      return 0;
    } else {
      // get the existing value that we have and merge
      VectorClockValuePair<SetLattice<string>> orig_pair;
      for (const auto& pair : original_value.vector_clock()) {
        orig_pair.vector_clock.insert(pair.first, pair.second);
      }
      for (auto& val : original_value.values()) {
        orig_pair.value.insert(std::move(val));
      }
      CausalPairLattice<SetLattice<string>> orig(orig_pair);

      VectorClockValuePair<SetLattice<string>> input_pair;
      for (const auto& pair : input_value.vector_clock()) {
        input_pair.vector_clock.insert(pair.first, pair.second);
      }
      for (auto& val : input_value.values()) {
        input_pair.value.insert(std::move(val));
      }
      CausalPairLattice<SetLattice<string>> input(input_pair);

      orig.merge(input);

      CausalValue new_value;
      auto ptr = new_value.mutable_vector_clock();
      // serialize vector clock
      for (const auto& pair : orig.reveal().vector_clock.reveal()) {
        (*ptr)[pair.first] = pair.second.reveal();
      }
      // serialize values
      // note that this creates unnecessary copy of val, but
      // we have to since the reveal() method is marked as "const"
      for (const string& val : orig.reveal().value.reveal()) {
        new_value.add_values(val);
      }

      // write out the new payload.
      std::fstream output(fname,
                          std::ios::out | std::ios::trunc | std::ios::binary);

      if (!new_value.SerializeToOstream(&output)) {
        std::cerr << "Failed to write payload" << std::endl;
      }
      return output.tellp();
    }
  }

  void remove(const Key& key) {
    string fname = ebs_root_ + "ebs_" + std::to_string(tid_) + "/" + key;

    if (std::remove(fname.c_str()) != 0) {
      std::cerr << "Error deleting file" << std::endl;
    }
  }
};

using SerializerMap =
    std::unordered_map<LatticeType, Serializer*, lattice_type_hash>;

struct PendingRequest {
  PendingRequest() {}
  PendingRequest(string type, LatticeType lattice_type, string payload,
                 Address addr, string response_id) :
      type_(type),
      lattice_type_(std::move(lattice_type)),
      payload_(std::move(payload)),
      addr_(addr),
      response_id_(response_id) {}

  string type_;
  LatticeType lattice_type_;
  string payload_;
  Address addr_;
  string response_id_;
};

struct PendingGossip {
  PendingGossip() {}
  PendingGossip(LatticeType lattice_type, string payload) :
      lattice_type_(std::move(lattice_type)),
      payload_(std::move(payload)) {}
  LatticeType lattice_type_;
  string payload_;
};

#endif  // SRC_INCLUDE_UTILS_SERVER_UTILS_HPP_
