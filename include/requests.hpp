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

#ifndef SRC_INCLUDE_REQUESTS_HPP_
#define SRC_INCLUDE_REQUESTS_HPP_

#include "zmq/socket_cache.hpp"
#include "zmq/zmq_util.hpp"

template <typename RES>
bool receive(zmq::socket_t& recv_socket, set<string>& request_ids,
             vector<RES>& responses) {
  zmq::message_t message;

  // We allow as many timeouts as there are requests that we made. We may want
  // to make this configurable at some point.
  unsigned timeout_limit = request_ids.size();

  while (true) {
    RES response;

    if (recv_socket.recv(&message)) {
      string serialized_resp = kZmqUtil->message_to_string(message);
      response.ParseFromString(serialized_resp);
      string resp_id = response.response_id();

      if (request_ids.find(resp_id) != request_ids.end()) {
        request_ids.erase(resp_id);
        responses.push_back(response);
      }

      if (request_ids.size() == 0) {
        return true;
      }
    } else {
      // We assume that the request timed out here, so errno should always equal
      // EAGAIN. If that is not the case, log or print the errno here for
      // debugging.
      timeout_limit--;

      if (timeout_limit == 0) {
        responses.clear();
        return false;
      }
    }
  }
}

template <typename REQ>
void send_request(const REQ& request, zmq::socket_t& send_socket) {
  string serialized_req;
  request.SerializeToString(&serialized_req);
  kZmqUtil->send_string(serialized_req, &send_socket);
}

// Synchronous combination of send and receive.
template <typename REQ, typename RES>
RES make_request(const REQ& request, zmq::socket_t& send_socket,
                 zmq::socket_t& recv_socket, bool& succeed) {
  send_request<REQ>(request, send_socket);

  vector<RES> responses;
  set<string> req_ids{request.request_id()};
  succeed = receive<RES>(recv_socket, req_ids, responses);

  if (succeed) {
    return responses[0];
  } else {
    return RES();
  }
}

#endif  // SRC_INCLUDE_REQUESTS_HPP_
