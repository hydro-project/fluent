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

#ifndef SRC_INCLUDE_CLIENT_HPP_
#define SRC_INCLUDE_CLIENT_HPP_

#include "hash_ring.hpp"
#include "requests.hpp"
#include "requests.pb.h"
#include "spdlog/spdlog.h"
#include "threads.hpp"
#include "types.hpp"
#include "zmq/socket_cache.hpp"
#include "zmq/zmq_util.hpp"

unsigned ELB_BASE = 6000;

class KvsClient {
 public:
  /**
   * TODO(vikram): If local is true, we only ever use the first routing
   * address because we assume it's an ELB address. We should figure out how
   * to make sure that we sidestep that dependency in the future, but I
   * don't have to figure it out right now.
   *
   * @addrs A vector of routing addresses. If the cluster is running in local
   * mode, we will only use the first address
   * @routing_thread_count The number of thread sone ach routing node
   * @ip My node's IP address
   * @tid My client's thread ID
   * @timeout Length of request timeouts in ms
   * @local Whether the client is running within the Kubernetes cluster (true)
   * or not (false)
   */
  KvsClient(std::vector<Address> addrs, unsigned routing_thread_count,
            std::string ip, unsigned tid = 0, unsigned timeout = 10000,
            bool local = false) :
      route_addrs_(addrs),
      ut_(UserThread(ip, tid)),
      local_(local),
      routing_thread_count_(routing_thread_count),
      context_(zmq::context_t(1)),
      socket_cache_(SocketCache(&context_, ZMQ_PUSH)),
      key_address_puller_(zmq::socket_t(context_, ZMQ_PULL)),
      response_puller_(zmq::socket_t(context_, ZMQ_PULL)),
      logger_(spdlog::basic_logger_mt("client_log", "client_log.txt", true)) {
    // initialize logger
    logger_->flush_on(spdlog::level::info);

    // set class variables
    local_ = true;
    bad_response_.set_response_id("NULL_ERROR");

    std::hash<std::string> hasher;
    seed_ = time(NULL);
    seed_ += hasher(ip);
    seed_ += tid;
    logger_->info("Random seed is {}.", seed_);

    // bind the two sockets we listen on
    key_address_puller_.bind(ut_.get_key_address_bind_addr());
    key_address_puller_.setsockopt(ZMQ_RCVTIMEO, &timeout, sizeof(timeout));

    response_puller_.bind(ut_.get_request_pulling_bind_addr());
    response_puller_.setsockopt(ZMQ_RCVTIMEO, &timeout, sizeof(timeout));

    // set the request ID to 0
    rid_ = 0;
  }

  ~KvsClient() {}

 public:
  /**
   * Issue a GET request to the KVS.
   *
   * We return a decoded string as a response, but if no worker threads are
   * contactable from our client, we will either recurse infinitely while
   * retrying the request or return NULL. Since no trial_limit is specified, we
   * use a default value of 10.
   */
  std::string get(Key key) { return get(key, 10); }

  /**
   * Issue a GET request to the KVS.
   *
   * We return a decoded string as a response, but if no worker threads are
   * contactable from our client, we will either recurse infinitely while
   * retrying the request or return NULL. We attempt this request up to
   * trial_limit times before giving up.
   */
  std::string get(Key key, unsigned trial_limit) {
    KeyRequest request;
    prepare_data_request(request, key);
    request.set_type(get_request_type("GET"));

    KeyResponse response = try_request(request, trial_limit);

    if (is_error_response(response)) {
      return "";
    }

    KeyTuple tuple = response.tuples(0);
    if (tuple.error() == 1) {
      logger_->info("Key {} does not exist and could not be retrieved.", key);
      return "";
    }

    return tuple.payload();
  }

  /**
   * Issue a PUT request to the KVS.
   *
   * We return a decoded string as a response, but if no worker threads are
   * contactable from our client, we will either recurse infinitely while
   * retrying the request or return NULL. Since no trial_limit is specified, we
   * use a default value of 10.
   */
  bool put(Key key, std::string value) { return put(key, value, 10); }

  /**
   * Issue a PUT request to the KVS.
   *
   * We return a decoded string as a response, but if no worker threads are
   * contactable from our client, we will either recurse infinitely while
   * retrying the request or return NULL. We attempt this request up to
   * trial_limit times before giving up.
   */
  bool put(Key key, std::string value, unsigned trial_limit) {
    KeyRequest request;
    KeyTuple* tuple = prepare_data_request(request, key);
    request.set_type(get_request_type("PUT"));
    tuple->set_payload(value);

    KeyResponse response = try_request(request, trial_limit);

    return !is_error_response(response);
  }

  /**
   * Issue a durable PUT request to the KVS.
   *
   * This method issues a PUT request to all of the replicas of a particular
   * key in the KVS. It only returns true if all of the requests are
   * successful. Since no trial_limit is specified, we use a default value of
   * 5.
   */
  bool put_all(Key key, std::string value) { return put_all(key, value, 5); }

  /**
   * Issue a durable PUT request to the KVS.
   *
   * This method issues a PUT request to all of the replicas of a particular
   * key in the KVS. It only returns true if all of the requests are
   * successful. We attempt this request trial_limit times before giving up.
   * 5.
   */
  bool put_all(Key key, std::string value, unsigned trial_limit) {
    KeyRequest request;
    KeyTuple* tuple = prepare_data_request(request, key);
    request.set_type(get_request_type("PUT"));
    tuple->set_payload(value);

    std::vector<KeyResponse> responses =
        try_multi_request(request, trial_limit);

    return responses.size() != 0;
  }

  /**
   * Retrieve all replicas of a key from the KVS.
   *
   * This method issues a GET request to every replica responsible for a key.
   * If it gets a response from all, it returns a vector of the responses and
   * otherwise returns an empty vector. Since no trial_limit is specified, we
   * use a default value of 5.
   */
  std::vector<std::string> get_all(Key key) { return get_all(key, 5); }

  /**
   * Retrieve all replicas of a key from the KVS.
   *
   * This method issues a GET request to every replica responsible for a key.
   * If it gets a response from all, it returns a vector of the responses and
   * otherwise returns an empty vector. We attempt this request trial_limit
   * times before giving up.
   */
  std::vector<std::string> get_all(Key key, unsigned trial_limit) {
    KeyRequest request;
    prepare_data_request(request, key);
    request.set_type(get_request_type("GET"));

    std::vector<KeyResponse> responses =
        try_multi_request(request, trial_limit);
    std::vector<std::string> result;

    if (responses.size() == 0) {
      return result;
    }

    for (KeyResponse response : responses) {
      KeyTuple tuple = response.tuples(0);
      if (tuple.error() == 1) {
        logger_->info("Key {} does not exist and could not be retrieved.", key);
        result.clear();
        return result;
      }

      result.push_back(tuple.payload());
    }

    return result;
  }

  /**
   * Set the logger used by the client.
   */
  void set_logger(std::shared_ptr<spdlog::logger> logger) { logger_ = logger; }

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
   * A recursive helper method for the get_all and put_all implementations that
   * tries to issue a request at most trial_limit times before giving up. It
   * checks for the default failure modes (timeout, errno == 2, and cache
   * invalidation). If there are no issues, it returns the set of responses to
   * the respective implementations for them to deal with.
   */
  std::vector<KeyResponse> try_multi_request(KeyRequest request,
                                             unsigned trial_limit) {
    std::vector<KeyResponse> responses;
    if (trial_limit == 0) {
      return responses;
    }

    // we only get NULL back for the worker thread if the query to the routing
    // tier timed out, which should never happen.
    std::unordered_set<Address> workers =
        get_all_worker_threads(request.tuples(0).key());
    if (workers.size() != 0) {
      return responses;
    }

    std::unordered_set<std::string> request_ids;
    for (const Address& worker : workers) {
      std::string rid_str = get_request_id();
      request.set_request_id(rid_str);
      request_ids.insert(rid_str);

      send_request<KeyRequest>(request, socket_cache_[worker]);
    }

    bool succeed =
        receive<KeyResponse>(response_puller_, request_ids, responses);

    if (succeed) {
      logger_->info(
          "Request timed out while querying worker. Clearing address cache due "
          "to possible membership change and retrying request.");
      for (Address worker : workers) {
        invalidate_cache_for_worker(worker);
      }

      return try_multi_request(request, trial_limit - 1);
    }

    for (const KeyResponse& response : responses) {
      KeyTuple tuple = response.tuples(0);

      if (check_tuple(tuple)) {
        return try_multi_request(request, trial_limit - 1);
      }
    }

    return responses;
  }

  /**
   * A recursive helper method for the get and put implementations that tries
   * to issue a request at most trial_limit times before giving up. It  checks
   * for the default failure modes (timeout, errno == 2, and cache
   * invalidation). If there are no issues, it returns the set of responses to
   * the respective implementations for them to deal with. This is the same as
   * the above implementation of try_multi_request, except it only operates on
   * a single request.
   */
  KeyResponse try_request(KeyRequest request, unsigned trial_limit) {
    if (trial_limit == 0) {
      return bad_response_;
    }

    // Update request ID in case we are retrying, so we don't mistakenly get a
    // straggler response.
    request.set_request_id(get_request_id());

    // we only get NULL back for the worker thread if the query to the routing
    // tier timed out, which should never happen.
    Address worker = get_worker_thread(request.tuples(0).key());
    if (worker.length() == 0) {
      return bad_response_;
    }

    bool succeed;
    KeyResponse response = make_request<KeyRequest, KeyResponse>(
        request, socket_cache_[worker], response_puller_, succeed);

    while (!succeed) {
      logger_->info(
          "Request timed out while querying worker. Clearing address cache due "
          "to possible membership change and retrying request.");
      invalidate_cache_for_worker(worker);

      return try_request(request, trial_limit - 1);
    }

    KeyTuple tuple = response.tuples(0);
    if (check_tuple(tuple)) {
      try_request(request, trial_limit - 1);
    }

    return response;
  }

  /**
   * A helper method to check for the default failure modes for a request that
   * retrieves a response. It returns true if the caller method should reissue
   * the request (this happens if errno == 2). Otherwise, it returns false. It
   * invalidates the local cache if the information is out of date.
   */
  bool check_tuple(KeyTuple tuple) {
    Key key = tuple.key();
    if (tuple.error() == 2) {
      logger_->info(
          "Server ordered invalidation of key address cache for key {}. "
          "Retrying request.",
          key);

      invalidate_cache_for_key(key, tuple);
      return true;
    }

    if (tuple.has_invalidate() && tuple.invalidate()) {
      invalidate_cache_for_key(key, tuple);

      logger_->info(
          "Server ordered invalidation of key address cache for key {}", key);
    }

    return false;
  }

  /**
   * When a server thread tells us to invalidate the cache for a key it's
   * because we likely have out of date information for that key; it sends us
   * the updated information for that key, and update our cache with that
   * information.
   */
  void invalidate_cache_for_key(Key key, KeyTuple tuple) {
    key_address_cache_.erase(key);
    std::unordered_set<Address> new_cache;

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
  void invalidate_cache_for_worker(Address worker) {
    std::vector<std::string> tokens;
    split(worker, ':', tokens);
    std::string signature = tokens[1];
    std::unordered_set<Key> remove_set;

    for (const auto& key_pair : key_address_cache_) {
      for (const std::string& address : key_pair.second) {
        std::vector<std::string> v;
        split(address, ':', v);

        if (v[1] == signature) {
          remove_set.insert(key_pair.first);
        }
      }
    }

    for (const std::string& key : remove_set) {
      key_address_cache_.erase(key);
    }
  }

  /**
   * Prepare a data request object by populating the request ID, the key for
   * the request, and the response address. This method modifies the passed-in
   * KeyRequest and also returns a pointer to the KeyTuple contained by this
   * request.
   */
  KeyTuple* prepare_data_request(KeyRequest& request, Key key) {
    request.set_request_id(get_request_id());
    request.set_response_address(ut_.get_request_pulling_connect_addr());

    KeyTuple* tp = request.add_tuples();
    tp->set_key(key);
    tp->set_address_cache_size(key_address_cache_[key].size());

    return tp;
  }

  /**
   * returns all the worker threads for the key queried. If there are no cached
   * threads, a request is sent to the routing tier. If the query times out,
   * NULL is returned.
   */
  std::unordered_set<Address> get_all_worker_threads(Key key) {
    if (key_address_cache_.find(key) == key_address_cache_.end()) {
      std::unordered_set<Address> addresses = query_routing(key);

      if (addresses.size() == 0) {
        logger_->error(
            "Request to routing tier unexpectedly timed out. This should never "
            "happen!");
        return addresses;
      }

      key_address_cache_[key] = addresses;
    }

    return key_address_cache_[key];
  }

  /**
   * Similar to the previous method, but only returns one (randomly chosen)
   * worker address instead of all of them.
   */
  Address get_worker_thread(Key key) {
    std::unordered_set<Address> local_cache = get_all_worker_threads(key);

    // This will be empty if the request timed out in the get_all_worker_threads
    // method.
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
    unsigned thread_id = rand_r(&seed_) % routing_thread_count_;

    if (local_) {
      Address routing_ip = route_addrs_[rand_r(&seed_) % route_addrs_.size()];
      return RoutingThread(routing_ip, thread_id)
          .get_key_address_connect_addr();
    } else {
      Address routing_ip = route_addrs_[0];

      return "tcp://" + routing_ip + ":" + std::to_string(ELB_BASE + thread_id);
    }
  }

  /**
   * Send a query to the routing tier for the key passed in. If the query times
   * out, we return an empty result to the user. The only kind of error we can
   * get from the routing tier is an insufficient number of nodes in the
   * cluster -- in that case, we simply retry the request because we are
   * waiting for more nodes to join.
   */
  std::unordered_set<Address> query_routing(Key key) {
    int count = 0;

    // define protobuf request/response objects
    KeyAddressRequest request;
    KeyAddressResponse response;

    // populate request with response address, request id, etc.
    request.set_request_id(get_request_id());
    request.set_response_address(ut_.get_key_address_connect_addr());
    request.add_keys(key);

    std::unordered_set<Address> result;

    int error = -1;

    bool succeed;
    while (error != 0) {
      if (error == 1) {
        std::cerr << "No servers have joined the cluster yet. Retrying request."
                  << std::endl;
      }

      if (count > 0 && count % 5 == 0) {
        std::cerr << "Pausing for 5 seconds before continuing to query routing "
                     "layer..."
                  << std::endl;
        usleep(5000000);
      }

      // send the actual query to the routing tier
      response = make_request<KeyAddressRequest, KeyAddressResponse>(
          request, socket_cache_[get_routing_thread()], key_address_puller_,
          succeed);

      if (!succeed) {
        return result;
      } else {
        error = response.error();
      }

      count++;
    }

    // construct and return the unordered_set of IP adddresses.
    for (const std::string& ip : response.addresses(0).ips()) {
      result.insert(ip);
    }

    return result;
  }

  /**
   * Generates a unique request ID.
   */
  std::string get_request_id() {
    if (++rid_ % 10000 == 0) rid_ = 0;
    return ut_.get_ip() + ":" + std::to_string(ut_.get_tid()) + "_" +
           std::to_string(rid_++);
  }

  bool is_error_response(KeyResponse response) {
    return response.response_id() == bad_response_.response_id();
  }

 private:
  // the set of routing addresses outside the cluster
  std::vector<std::string> route_addrs_;

  // whether we are running this client in local or remote mode;
  bool local_;

  // the current request id
  unsigned rid_;

  // the random seed for this client
  unsigned seed_;

  // number of routing threads per routing IP
  unsigned routing_thread_count_;

  // the IP and port functions for this thread
  UserThread ut_;

  // the ZMQ context we use to create sockets
  zmq::context_t context_;

  // cache for opened sockets
  SocketCache socket_cache_;

  // ZMQ receiving sockets
  zmq::socket_t key_address_puller_;
  zmq::socket_t response_puller_;

  // cache for retrieved worker addresses organized by key
  std::unordered_map<Key, std::unordered_set<Address>> key_address_cache_;

  // class logger
  std::shared_ptr<spdlog::logger> logger_;

  // create a default response for a local error (ie, timeout or trial limit)
  KeyResponse bad_response_;
};

#endif  // SRC_INCLUDE_CLIENT_HPP_
