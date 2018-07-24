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

#include "socket_cache.hpp"

#include <utility>

zmq::socket_t& SocketCache::At(const Address& addr) {
  auto iter = cache_.find(addr);
  if (iter != cache_.end()) {
    return iter->second;
  }

  zmq::socket_t socket(*context_, type_);
  socket.connect(addr);
  auto p = cache_.insert(std::make_pair(addr, std::move(socket)));

  return p.first->second;
}

zmq::socket_t& SocketCache::operator[](const Address& addr) { return At(addr); }

void SocketCache::clear_cache() { cache_.clear(); }
