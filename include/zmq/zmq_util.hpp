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

#ifndef SRC_INCLUDE_ZMQ_ZMQ_UTIL_HPP_
#define SRC_INCLUDE_ZMQ_ZMQ_UTIL_HPP_

#include <cstring>
#include <ostream>
#include <string>
#include <utility>
#include <vector>

#include "zmq.hpp"

class ZmqUtilInterface {
 public:
  // Converts the data within a `zmq::message_t` into a string.
  std::string message_to_string(const zmq::message_t& message);
  // Converts a string into a `zmq::message_t`.
  zmq::message_t string_to_message(const std::string& s);
  // `send` a string over the socket.
  virtual void send_string(const std::string& s, zmq::socket_t* socket) = 0;
  // `recv` a string over the socket.
  virtual std::string recv_string(zmq::socket_t* socket) = 0;
  // `poll` is a wrapper around `zmq::poll` that takes a vector instead of a
  // pointer and a size.
  virtual int poll(long timeout, std::vector<zmq::pollitem_t>* items) = 0;
};

class ZmqUtil : public ZmqUtilInterface {
 public:
  virtual void send_string(const std::string& s, zmq::socket_t* socket);
  virtual std::string recv_string(zmq::socket_t* socket);
  virtual int poll(long timeout, std::vector<zmq::pollitem_t>* items);
};

extern ZmqUtilInterface* kZmqUtil;

#endif  // SRC_INCLUDE_ZMQ_ZMQ_UTIL_HPP_
