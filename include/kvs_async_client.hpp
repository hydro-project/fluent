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

#ifndef SRC_INCLUDE_ASYNC_CLIENT_HPP_
#define SRC_INCLUDE_ASYNC_CLIENT_HPP_

#include "common.hpp"
#include "kvs.pb.h"
#include "requests.hpp"
#include "threads.hpp"
#include "types.hpp"

using TimePoint = std::chrono::time_point<std::chrono::system_clock>;

struct PendingRequest {
  TimePoint tp_;
  Address worker_addr_;
  KeyRequest request_;
};

class KvsAsyncClientInterface {
 public:
  virtual string put_async(const Key& key, const string& payload,
                           LatticeType lattice_type) = 0;
  virtual void get_async(const Key& key) = 0;
  virtual vector<KeyResponse> receive_async(ZmqUtilInterface* kZmqUtil) = 0;
  virtual zmq::context_t* get_context() = 0;
};

class KvsAsyncClient : public KvsAsyncClientInterface {
 public:
  /**
   * @addrs A vector of routing addresses.
   * @routing_thread_count The number of thread sone ach routing node
   * @ip My node's IP address
   * @tid My client's thread ID
   * @timeout Length of request timeouts in ms
   */
  KvsAsyncClient(vector<UserRoutingThread> routing_threads, string ip,
                 unsigned tid = 0, unsigned timeout = 10000) :
      routing_threads_(routing_threads),
      ut_(UserThread(ip, tid)),
      context_(zmq::context_t(1)),
      socket_cache_(SocketCache(&context_, ZMQ_PUSH)),
      key_address_puller_(zmq::socket_t(context_, ZMQ_PULL)),
      response_puller_(zmq::socket_t(context_, ZMQ_PULL)),
      log_(spdlog::basic_logger_mt("client_log", "client_log.txt", true)),
      timeout_(timeout) {
    // initialize logger
    log_->flush_on(spdlog::level::info);

    std::hash<string> hasher;
    seed_ = time(NULL);
    seed_ += hasher(ip);
    seed_ += tid;
    log_->info("Random seed is {}.", seed_);

    // bind the two sockets we listen on
    key_address_puller_.bind(ut_.key_address_bind_address());
    response_puller_.bind(ut_.response_bind_address());

    pollitems_ = {
        {static_cast<void*>(key_address_puller_), 0, ZMQ_POLLIN, 0},
        {static_cast<void*>(response_puller_), 0, ZMQ_POLLIN, 0},
    };

    // set the request ID to 0
    rid_ = 0;
  }

  ~KvsAsyncClient() {}

 public:
  /**
   * Issue an async PUT request to the KVS for a certain lattice typed value.
   */
  string put_async(const Key& key, const string& payload,
                   LatticeType lattice_type) {
    KeyRequest request;
    KeyTuple* tuple = prepare_data_request(request, key);
    request.set_type(RequestType::PUT);
    tuple->set_lattice_type(lattice_type);
    tuple->set_payload(payload);

    try_request(request);
    return request.request_id();
  }

  /**
   * Issue an async GET request to the KVS.
   */
  void get_async(const Key& key) {
    // we issue GET only when it is not in the pending map
    if (pending_get_response_map_.find(key) ==
        pending_get_response_map_.end()) {
      KeyRequest request;
      prepare_data_request(request, key);
      request.set_type(RequestType::GET);

      try_request(request);
    }
  }

  vector<KeyResponse> receive_async(ZmqUtilInterface* kZmqUtil) {
    vector<KeyResponse> result;
    kZmqUtil->poll(0, &pollitems_);

    // handle a key address response
    if (pollitems_[0].revents & ZMQ_POLLIN) {
      string serialized = kZmqUtil->recv_string(&key_address_puller_);
      KeyAddressResponse response;
      response.ParseFromString(serialized);
      Key key = response.addresses(0).key();

      if (pending_request_map_.find(key) != pending_request_map_.end()) {
        if (response.error() == 1) {
          log_->error(
              "No servers have joined the cluster yet. Retrying request.");
          pending_request_map_[key].first = std::chrono::system_clock::now();

          query_routing_async(key);
        } else {
          // populate cache
          for (const Address& ip : response.addresses(0).ips()) {
            key_address_cache_[key].insert(ip);
          }

          // handle stuff in pending request map
          for (auto& req : pending_request_map_[key].second) {
            try_request(req);
          }

          // GC the pending request map
          pending_request_map_.erase(key);
        }
      }
    }

    if (pollitems_[1].revents & ZMQ_POLLIN) {
      string serialized = kZmqUtil->recv_string(&response_puller_);
      KeyResponse response;
      response.ParseFromString(serialized);
      Key key = response.tuples(0).key();

      if (response.type() == RequestType::GET) {
        if (pending_get_response_map_.find(key) !=
            pending_get_response_map_.end()) {
          if (check_tuple(response.tuples(0))) {
            // error no == 2, so re-issue request
            pending_get_response_map_[key].tp_ =
                std::chrono::system_clock::now();

            try_request(pending_get_response_map_[key].request_);
          } else {
            // error no == 0 or 1
            result.push_back(response);
            pending_get_response_map_.erase(key);
          }
        }
      } else {
        if (pending_put_response_map_.find(key) !=
                pending_put_response_map_.end() &&
            pending_put_response_map_[key].find(response.response_id()) !=
                pending_put_response_map_[key].end()) {
          if (check_tuple(response.tuples(0))) {
            // error no == 2, so re-issue request
            pending_put_response_map_[key][response.response_id()].tp_ =
                std::chrono::system_clock::now();

            try_request(pending_put_response_map_[key][response.response_id()]
                            .request_);
          } else {
            // error no == 0
            result.push_back(response);
            pending_put_response_map_[key].erase(response.response_id());

            if (pending_put_response_map_[key].size() == 0) {
              pending_put_response_map_.erase(key);
            }
          }
        }
      }
    }

    // GC the pending request map
    set<Key> to_remove;
    for (const auto& pair : pending_request_map_) {
      if (std::chrono::duration_cast<std::chrono::milliseconds>(
              std::chrono::system_clock::now() - pair.second.first)
              .count() > timeout_) {
        // query to the routing tier timed out
        for (const auto& req : pair.second.second) {
          result.push_back(generate_bad_response(req));
        }

        to_remove.insert(pair.first);
      }
    }

    for (const Key& key : to_remove) {
      pending_request_map_.erase(key);
    }

    // GC the pending get response map
    to_remove.clear();
    for (const auto& pair : pending_get_response_map_) {
      if (std::chrono::duration_cast<std::chrono::milliseconds>(
              std::chrono::system_clock::now() - pair.second.tp_)
              .count() > timeout_) {
        // query to server timed out
        result.push_back(generate_bad_response(pair.second.request_));
        to_remove.insert(pair.first);
        invalidate_cache_for_worker(pair.second.worker_addr_);
      }
    }

    for (const Key& key : to_remove) {
      pending_get_response_map_.erase(key);
    }

    // GC the pending put response map
    map<Key, set<string>> to_remove_put;
    for (const auto& key_map_pair : pending_put_response_map_) {
      for (const auto& id_map_pair :
           pending_put_response_map_[key_map_pair.first]) {
        if (std::chrono::duration_cast<std::chrono::milliseconds>(
                std::chrono::system_clock::now() -
                pending_put_response_map_[key_map_pair.first][id_map_pair.first]
                    .tp_)
                .count() > timeout_) {
          result.push_back(generate_bad_response(id_map_pair.second.request_));
          to_remove_put[key_map_pair.first].insert(id_map_pair.first);
          invalidate_cache_for_worker(id_map_pair.second.worker_addr_);
        }
      }
    }

    for (const auto& key_set_pair : to_remove_put) {
      for (const auto& id : key_set_pair.second) {
        pending_put_response_map_[key_set_pair.first].erase(id);
      }
    }

    return result;
  }

  /**
   * Set the logger used by the client.
   */
  void set_logger(logger log) { log_ = log; }

  /**
   * Clears the key address cache held by this client.
   */
  void clear_cache() { key_address_cache_.clear(); }

  /**
   * Return the ZMQ context used by this client.
   */
  zmq::context_t* get_context() { return &context_; }

  /**
   * Return the random seed used by this client.
   */
  unsigned get_seed() { return seed_; }

 private:
  /**
   * A recursive helper method for the get and put implementations that tries
   * to issue a request at most trial_limit times before giving up. It  checks
   * for the default failure modes (timeout, errno == 2, and cache
   * invalidation). If there are no issues, it returns the set of responses to
   * the respective implementations for them to deal with. This is the same as
   * the above implementation of try_multi_request, except it only operates on
   * a single request.
   */
  void try_request(KeyRequest& request) {
    // we only get NULL back for the worker thread if the query to the routing
    // tier timed out, which should never happen.
    Key key = request.tuples(0).key();
    Address worker = get_worker_thread(key);
    if (worker.length() == 0) {
      // this means a key addr request is issued asynchronously
      if (pending_request_map_.find(key) == pending_request_map_.end()) {
        pending_request_map_[key].first = std::chrono::system_clock::now();
      }
      pending_request_map_[key].second.push_back(request);
      return;
    }

    request.mutable_tuples(0)->set_address_cache_size(
        key_address_cache_[key].size());

    send_request<KeyRequest>(request, socket_cache_[worker]);

    if (request.type() == RequestType::GET) {
      if (pending_get_response_map_.find(key) ==
          pending_get_response_map_.end()) {
        pending_get_response_map_[key].tp_ = std::chrono::system_clock::now();
        pending_get_response_map_[key].request_ = request;
      }

      pending_get_response_map_[key].worker_addr_ = worker;
    } else {
      if (pending_put_response_map_[key].find(request.request_id()) ==
          pending_put_response_map_[key].end()) {
        pending_put_response_map_[key][request.request_id()].tp_ =
            std::chrono::system_clock::now();
        pending_put_response_map_[key][request.request_id()].request_ = request;
      }
      pending_put_response_map_[key][request.request_id()].worker_addr_ =
          worker;
    }
  }

  /**
   * A helper method to check for the default failure modes for a request that
   * retrieves a response. It returns true if the caller method should reissue
   * the request (this happens if errno == 2). Otherwise, it returns false. It
   * invalidates the local cache if the information is out of date.
   */
  bool check_tuple(const KeyTuple& tuple) {
    Key key = tuple.key();
    if (tuple.error() == 2) {
      log_->info(
          "Server ordered invalidation of key address cache for key {}. "
          "Retrying request.",
          key);

      invalidate_cache_for_key(key, tuple);
      return true;
    }

    if (tuple.has_invalidate() && tuple.invalidate()) {
      invalidate_cache_for_key(key, tuple);

      log_->info("Server ordered invalidation of key address cache for key {}",
                 key);
    }

    return false;
  }

  /**
   * When a server thread tells us to invalidate the cache for a key it's
   * because we likely have out of date information for that key; it sends us
   * the updated information for that key, and update our cache with that
   * information.
   */
  void invalidate_cache_for_key(const Key& key, const KeyTuple& tuple) {
    key_address_cache_.erase(key);
    set<Address> new_cache;

    for (const Address address : tuple.addresses()) {
      new_cache.insert(address);
    }

    key_address_cache_[key] = new_cache;
  }

  /**
   * Invalidate the key caches for any key that previously had this worker in
   * its cache. The underlying assumption is that if the worker timed out, it
   * might have failed, and so we don't want to rely on it being alive for both
   * the key we were querying and any other key.
   */
  void invalidate_cache_for_worker(const Address& worker) {
    vector<string> tokens;
    split(worker, ':', tokens);
    string signature = tokens[1];
    set<Key> remove_set;

    for (const auto& key_pair : key_address_cache_) {
      for (const string& address : key_pair.second) {
        vector<string> v;
        split(address, ':', v);

        if (v[1] == signature) {
          remove_set.insert(key_pair.first);
        }
      }
    }

    for (const string& key : remove_set) {
      key_address_cache_.erase(key);
    }
  }

  /**
   * Prepare a data request object by populating the request ID, the key for
   * the request, and the response address. This method modifies the passed-in
   * KeyRequest and also returns a pointer to the KeyTuple contained by this
   * request.
   */
  KeyTuple* prepare_data_request(KeyRequest& request, const Key& key) {
    request.set_request_id(get_request_id());
    request.set_response_address(ut_.response_connect_address());

    KeyTuple* tp = request.add_tuples();
    tp->set_key(key);

    return tp;
  }

  /**
   * returns all the worker threads for the key queried. If there are no cached
   * threads, a request is sent to the routing tier. If the query times out,
   * NULL is returned.
   */
  set<Address> get_all_worker_threads(const Key& key) {
    if (key_address_cache_.find(key) == key_address_cache_.end() ||
        key_address_cache_[key].size() == 0) {
      if (pending_request_map_.find(key) == pending_request_map_.end()) {
        query_routing_async(key);
      }
      return set<Address>();
    } else {
      return key_address_cache_[key];
    }
  }

  /**
   * Similar to the previous method, but only returns one (randomly chosen)
   * worker address instead of all of them.
   */
  Address get_worker_thread(const Key& key) {
    set<Address> local_cache = get_all_worker_threads(key);

    // This will be empty if the worker threads are not cached locally
    if (local_cache.size() == 0) {
      return "";
    }

    return *(next(begin(local_cache), rand_r(&seed_) % local_cache.size()));
  }

  /**
   * Returns one random routing thread's key address connection address. If the
   * client is running outside of the cluster (ie, it is querying the ELB),
   * there's only one address to choose from but 4 threads.
   */
  Address get_routing_thread() {
    return routing_threads_[rand_r(&seed_) % routing_threads_.size()]
        .key_address_connect_address();
  }

  /**
   * Send a query to the routing tier asynchronously.
   */
  void query_routing_async(const Key& key) {
    // define protobuf request objects
    KeyAddressRequest request;

    // populate request with response address, request id, etc.
    request.set_request_id(get_request_id());
    request.set_response_address(ut_.key_address_connect_address());
    request.add_keys(key);

    Address rt_thread = get_routing_thread();
    send_request<KeyAddressRequest>(request, socket_cache_[rt_thread]);
  }

  /**
   * Generates a unique request ID.
   */
  string get_request_id() {
    if (++rid_ % 10000 == 0) rid_ = 0;
    return ut_.ip() + ":" + std::to_string(ut_.tid()) + "_" +
           std::to_string(rid_++);
  }

  KeyResponse generate_bad_response(const KeyRequest& req) {
    KeyResponse resp;

    resp.set_type(req.type());
    resp.set_response_id(req.request_id());
    resp.set_error(ResponseErrorType::TIMEOUT);

    KeyTuple* tp = resp.add_tuples();
    tp->set_key(req.tuples(0).key());

    if (req.type() == RequestType::PUT) {
      tp->set_lattice_type(req.tuples(0).lattice_type());
      tp->set_payload(req.tuples(0).payload());
    }

    return resp;
  }

 private:
  // the set of routing addresses outside the cluster
  vector<UserRoutingThread> routing_threads_;

  // the current request id
  unsigned rid_;

  // the random seed for this client
  unsigned seed_;

  // the IP and port functions for this thread
  UserThread ut_;

  // the ZMQ context we use to create sockets
  zmq::context_t context_;

  // cache for opened sockets
  SocketCache socket_cache_;

  // ZMQ receiving sockets
  zmq::socket_t key_address_puller_;
  zmq::socket_t response_puller_;

  vector<zmq::pollitem_t> pollitems_;

  // cache for retrieved worker addresses organized by key
  map<Key, set<Address>> key_address_cache_;

  // class logger
  logger log_;

  // GC timeout
  unsigned timeout_;

  // keeps track of pending requests due to missing worker address
  map<Key, pair<TimePoint, vector<KeyRequest>>> pending_request_map_;

  // keeps track of pending get responses
  map<Key, PendingRequest> pending_get_response_map_;

  // keeps track of pending put responses
  map<Key, map<string, PendingRequest>> pending_put_response_map_;
};

#endif  // SRC_INCLUDE_ASYNC_CLIENT_HPP_
