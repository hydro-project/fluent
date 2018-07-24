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

#ifndef SRC_INCLUDE_ZMQ_SOCKET_CACHE_HPP_
#define SRC_INCLUDE_ZMQ_SOCKET_CACHE_HPP_

#include <map>
#include <string>

#include "types.hpp"
#include "zmq.hpp"

// A SocketCache is a map from ZeroMQ addresses to PUSH ZeroMQ sockets. The
// socket corresponding to address `address` can be retrieved from a
// SocketCache `cache` with `cache[address]` or `cache.At(address)`. If a
// socket with a given address is not in the cache when it is requested, one is
// created and connected to the address. An example:
//
//   zmq::context_t context(1);
//   SocketCache cache(&context);
//   // This will create a socket and connect it to "inproc://a".
//   zmq::socket_t& a = cache["inproc://a"];
//   // This will not createa new socket. It will return the socket created in
//   // the previous line. In other words, a and the_same_a_as_before are
//   // references to the same socket.
//   zmq::socket_t& the_same_a_as_before = cache["inproc://a"];
//   // cache.At("inproc://a") is 100% equivalent to cache["inproc://a"].
//   zmq::socket_t& another_a = cache.At("inproc://a");
class SocketCache {
 public:
  explicit SocketCache(zmq::context_t* context, int type) :
      context_(context),
      type_(type) {}
  zmq::socket_t& At(const Address& addr);
  zmq::socket_t& operator[](const Address& addr);
  void clear_cache();

 private:
  zmq::context_t* context_;
  std::map<Address, zmq::socket_t> cache_;
  int type_;
};

#endif  // SRC_INCLUDE_ZMQ_SOCKET_CACHE_HPP_
