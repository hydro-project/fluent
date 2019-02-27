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

#ifndef INCLUDE_THREADS_HPP_
#define INCLUDE_THREADS_HPP_

#include "types.hpp"

// define routing base ports
const unsigned kKeyAddressPort = 6450;

// used by user nodes
const unsigned kUserResponsePort = 6800;
const unsigned kUserKeyAddressPort = 6850;
const unsigned kBenchmarkCommandPort = 6900;

// used by the management node
const unsigned kKopsRestartCountPort = 7000;

// used by the cache system
const unsigned kCacheUpdatePort = 7150;

const string kBindBase = "tcp://*:";

class CacheThread {
  Address ip_;
  Address ip_base_;
  unsigned tid_;

 public:
  CacheThread(Address ip, unsigned tid) :
      ip_(ip),
      ip_base_("ipc://" + ip_ + ":"),
      tid_(tid) {}

  Address ip() const { return ip_; }

  unsigned tid() const { return tid_; }

  Address cache_get_bind_address() const { return "ipc:///requests/get"; }

  Address cache_get_connect_address() const { return "ipc:///requests/get"; }

  Address cache_put_bind_address() const { return "ipc:///requests/put"; }

  Address cache_put_connect_address() const { return "ipc:///requests/put"; }

  Address cache_update_bind_address() const {
    return ip_base_ + std::to_string(tid_ + kCacheUpdatePort);
  }

  Address cache_update_connect_address() const {
    return ip_base_ + std::to_string(tid_ + kCacheUpdatePort);
  }
};

class UserRoutingThread {
  Address ip_;
  Address ip_base_;
  unsigned tid_;

 public:
  UserRoutingThread() {}

  UserRoutingThread(Address ip, unsigned tid) :
      ip_(ip),
      tid_(tid),
      ip_base_("tcp://" + ip_ + ":") {}

  Address ip() const { return ip_; }

  unsigned tid() const { return tid_; }

  Address key_address_connect_address() const {
    return ip_base_ + std::to_string(tid_ + kKeyAddressPort);
  }

  Address key_address_bind_address() const {
    return kBindBase + std::to_string(tid_ + kKeyAddressPort);
  }
};

class UserThread {
  Address ip_;
  Address ip_base_;
  unsigned tid_;

 public:
  UserThread() {}
  UserThread(Address ip, unsigned tid) :
      ip_(ip),
      tid_(tid),
      ip_base_("tcp://" + ip_ + ":") {}

  Address ip() const { return ip_; }

  unsigned tid() const { return tid_; }

  Address response_connect_address() const {
    return ip_base_ + std::to_string(tid_ + kUserResponsePort);
  }

  Address response_bind_address() const {
    return kBindBase + std::to_string(tid_ + kUserResponsePort);
  }

  Address key_address_connect_address() const {
    return ip_base_ + std::to_string(tid_ + kUserKeyAddressPort);
  }

  Address key_address_bind_address() const {
    return kBindBase + std::to_string(tid_ + kUserKeyAddressPort);
  }
};

#endif  // INCLUDE_THREADS_HPP_
