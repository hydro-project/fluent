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

template <typename REQ, typename RES>
bool recursive_receive(zmq::socket_t& receiving_socket, zmq::message_t& message,
                       const REQ& req, RES& response, bool& succeed) {
  bool rc = receiving_socket.recv(&message);

  if (rc) {
    auto serialized_resp = kZmqUtil->message_to_string(message);
    response.ParseFromString(serialized_resp);

    if (req.request_id() == response.response_id()) {
      succeed = true;
      return false;
    } else {
      return true;
    }
  } else {
    // timeout
    if (errno == EAGAIN) {
      succeed = false;
    } else {
      succeed = false;
    }

    return false;
  }
}

template <typename REQ, typename RES>
RES send_request(const REQ& req, zmq::socket_t& sending_socket,
                 zmq::socket_t& receiving_socket, bool& succeed) {
  std::string serialized_req;
  req.SerializeToString(&serialized_req);
  kZmqUtil->send_string(serialized_req, &sending_socket);

  RES response;
  zmq::message_t message;

  bool recurse = recursive_receive<REQ, RES>(receiving_socket, message, req,
                                             response, succeed);

  while (recurse) {
    response.Clear();
    zmq::message_t message;
    recurse = recursive_receive<REQ, RES>(receiving_socket, message, req,
                                          response, succeed);
  }

  return response;
}

#endif  // SRC_INCLUDE_REQUESTS_HPP_
