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

#include <stdlib.h>

#include "hash_ring.hpp"
#include "requests.hpp"
#include "spdlog/spdlog.h"
#include "threads.hpp"
#include "yaml-cpp/yaml.h"

unsigned kBenchmarkThreadNum;
unsigned kRoutingThreadCount;
unsigned kDefaultLocalReplication;

ZmqUtil zmq_util;
ZmqUtilInterface* kZmqUtil = &zmq_util;

HashRingUtil hash_ring_util;
HashRingUtilInterface* kHashRingUtil = &hash_ring_util;

double get_base(unsigned N, double skew) {
  double base = 0;
  for (unsigned k = 1; k <= N; k++) {
    base += pow(k, -1 * skew);
  }
  return (1 / base);
}

double get_zipf_prob(unsigned rank, double skew, double base) {
  return pow(rank, -1 * skew) / base;
}

int sample(int n, unsigned& seed, double base,
           std::unordered_map<unsigned, double>& sum_probs) {
  double z;            // Uniform random number (0 < z < 1)
  int zipf_value;      // Computed exponential value to be returned
  int i;               // Loop counter
  int low, high, mid;  // Binary-search bounds

  // Pull a uniform random number (0 < z < 1)
  do {
    z = rand_r(&seed) / static_cast<double>(RAND_MAX);
  } while ((z == 0) || (z == 1));

  // Map z to the value
  low = 1, high = n;

  do {
    mid = floor((low + high) / 2);
    if (sum_probs[mid] >= z && sum_probs[mid - 1] < z) {
      zipf_value = mid;
      break;
    } else if (sum_probs[mid] >= z) {
      high = mid - 1;
    } else {
      low = mid + 1;
    }
  } while (low <= high);

  // Assert that zipf_value is between 1 and N
  assert((zipf_value >= 1) && (zipf_value <= n));

  return zipf_value;
}

void handle_request(
    Key key, std::string value, SocketCache& pushers,
    std::vector<Address>& routing_addresses,
    std::unordered_map<Key, std::unordered_set<Address>>& key_address_cache,
    unsigned& seed, std::shared_ptr<spdlog::logger> logger, UserThread& ut,
    zmq::socket_t& response_puller, zmq::socket_t& key_address_puller,
    Address& ip, unsigned& thread_id, unsigned& rid, unsigned& trial) {
  if (trial > 5) {
    logger->info("Trial #{} for request for key {}.", trial, key);
    logger->info("Waiting 5 seconds.");
    std::chrono::seconds dura(5);
    std::this_thread::sleep_for(dura);
    logger->info("Waited 5s.");
  }

  // get worker address
  Address worker_address;
  if (key_address_cache.find(key) == key_address_cache.end()) {
    // query the routing and update the cache
    Address target_routing_address =
        kHashRingUtil
            ->get_random_routing_thread(routing_addresses, seed,
                                        kRoutingThreadCount)
            .get_key_address_connect_addr();

    bool succeed;
    std::vector<std::string> addresses =
        kHashRingUtil->get_address_from_routing(
            ut, key, pushers[target_routing_address], key_address_puller,
            succeed, ip, thread_id, rid);

    if (succeed) {
      for (const std::string& address : addresses) {
        key_address_cache[key].insert(address);
      }

      worker_address = addresses[rand_r(&seed) % addresses.size()];
    } else {
      logger->error(
          "Request timed out when querying routing. This should never happen!");
      return;
    }
  } else {
    if (key_address_cache[key].size() == 0) {
      logger->error("Address cache for key " + key + " has size 0.");
      return;
    }
    worker_address = *(next(begin(key_address_cache[key]),
                            rand_r(&seed) % key_address_cache[key].size()));
  }

  KeyRequest req;
  req.set_response_address(ut.get_request_pulling_connect_addr());

  std::string req_id =
      ip + ":" + std::to_string(thread_id) + "_" + std::to_string(rid);
  req.set_request_id(req_id);
  rid += 1;
  KeyTuple* tp = req.add_tuples();
  tp->set_key(key);
  tp->set_address_cache_size(key_address_cache[key].size());

  if (value == "") {
    // get request
    req.set_type(get_request_type("GET"));
  } else {
    // put request
    req.set_type(get_request_type("PUT"));
    tp->set_value(value);
    tp->set_timestamp(0);
  }

  bool succeed;
  auto res = send_request<KeyRequest, KeyResponse>(req, pushers[worker_address],
                                                   response_puller, succeed);

  if (succeed) {
    KeyTuple tuple = res.tuples(0);

    if (tuple.error() == 2) {
      trial += 1;
      if (trial > 5) {
        for (const auto& address : tuple.addresses()) {
          logger->info("Server's return address for key {} is {}.", key,
                       address);
        }

        for (const std::string& address : key_address_cache[key]) {
          logger->info("My cached address for key {} is {}.", key, address);
        }
      }

      // update cache and retry
      key_address_cache.erase(key);
      handle_request(key, value, pushers, routing_addresses, key_address_cache,
                     seed, logger, ut, response_puller, key_address_puller, ip,
                     thread_id, rid, trial);
    } else {
      // succeeded
      if (tuple.has_invalidate() && tuple.invalidate()) {
        // update cache
        key_address_cache.erase(key);
      }
    }
  } else {
    logger->info(
        "Request timed out when querying worker: clearing cache due to "
        "possible node membership changes.");
    // likely the node has departed. We clear the entries relavant to the
    // worker_address
    std::vector<std::string> tokens;
    split(worker_address, ':', tokens);
    std::string signature = tokens[1];
    std::unordered_set<Key> remove_set;

    for (const auto& key_pair : key_address_cache) {
      for (const std::string& address : key_pair.second) {
        std::vector<std::string> v;
        split(address, ':', v);

        if (v[1] == signature) {
          remove_set.insert(key_pair.first);
        }
      }
    }

    for (const std::string& key : remove_set) {
      key_address_cache.erase(key);
    }

    trial += 1;
    handle_request(key, value, pushers, routing_addresses, key_address_cache,
                   seed, logger, ut, response_puller, key_address_puller, ip,
                   thread_id, rid, trial);
  }
}

void run(unsigned thread_id, std::string ip,
         std::vector<Address> routing_addresses,
         std::vector<MonitoringThread> monitoring_threads) {
  std::string log_file = "log_" + std::to_string(thread_id) + ".txt";
  std::string logger_name = "benchmark_log_" + std::to_string(thread_id);
  auto logger = spdlog::basic_logger_mt(logger_name, log_file, true);
  logger->flush_on(spdlog::level::info);

  std::hash<std::string> hasher;
  unsigned seed = time(NULL);
  seed += hasher(ip);
  seed += thread_id;
  logger->info("Random seed is {}.", seed);

  // mapping from key to a set of worker addresses
  std::unordered_map<Key, std::unordered_set<Address>> key_address_cache;

  // observed per-key avg latency
  std::unordered_map<Key, std::pair<double, unsigned>> observed_latency;

  UserThread ut = UserThread(ip, thread_id);

  int timeout = 10000;
  zmq::context_t context(1);
  SocketCache pushers(&context, ZMQ_PUSH);

  // responsible for pulling response
  zmq::socket_t response_puller(context, ZMQ_PULL);
  response_puller.setsockopt(ZMQ_RCVTIMEO, &timeout, sizeof(timeout));
  response_puller.bind(ut.get_request_pulling_bind_addr());

  // responsible for receiving key address responses
  zmq::socket_t key_address_puller(context, ZMQ_PULL);
  key_address_puller.setsockopt(ZMQ_RCVTIMEO, &timeout, sizeof(timeout));
  key_address_puller.bind(ut.get_key_address_bind_addr());

  // responsible for pulling benchmark commands
  zmq::socket_t command_puller(context, ZMQ_PULL);
  command_puller.bind("tcp://*:" +
                      std::to_string(thread_id + kBenchmarkCommandBasePort));

  std::vector<zmq::pollitem_t> pollitems = {
      {static_cast<void*>(command_puller), 0, ZMQ_POLLIN, 0}};

  unsigned rid = 0;

  while (true) {
    kZmqUtil->poll(-1, &pollitems);

    if (pollitems[0].revents & ZMQ_POLLIN) {
      logger->info("Received benchmark command!");
      std::vector<std::string> v;

      split(kZmqUtil->recv_string(&command_puller), ':', v);
      std::string mode = v[0];

      if (mode == "CACHE") {
        unsigned num_keys = stoi(v[1]);
        // warm up cache
        key_address_cache.clear();
        auto warmup_start = std::chrono::system_clock::now();

        for (unsigned i = 1; i <= num_keys; i++) {
          // key is 8 bytes
          Key key = std::string(8 - std::to_string(i).length(), '0') +
                    std::to_string(i);

          if (i % 50000 == 0) {
            logger->info("warming up cache for key {}", key);
          }

          Address target_routing_address =
              kHashRingUtil
                  ->get_random_routing_thread(routing_addresses, seed,
                                              kRoutingThreadCount)
                  .get_key_address_connect_addr();
          bool succeed;
          std::vector<std::string> addresses =
              kHashRingUtil->get_address_from_routing(
                  ut, key, pushers[target_routing_address], key_address_puller,
                  succeed, ip, thread_id, rid);

          if (succeed) {
            for (const std::string address : addresses) {
              key_address_cache[key].insert(address);
            }
          } else {
            logger->info("Request timed out during cache warmup.");
          }
        }

        auto warmup_time = std::chrono::duration_cast<std::chrono::seconds>(
                               std::chrono::system_clock::now() - warmup_start)
                               .count();
        logger->info("Cache warm-up took {} seconds.", warmup_time);
      } else if (mode == "LOAD") {
        std::string type = v[1];
        unsigned num_keys = stoi(v[2]);
        unsigned length = stoi(v[3]);
        unsigned report_period = stoi(v[4]);
        unsigned time = stoi(v[5]);
        double contention = stod(v[6]);

        std::unordered_map<unsigned, double> sum_probs;
        double base;

        double zipf = contention;

        if (zipf > 0) {
          logger->info("Zipf coefficient is {}.", zipf);
          base = get_base(num_keys, zipf);
          sum_probs[0] = 0;

          for (unsigned i = 1; i <= num_keys; i++) {
            sum_probs[i] = sum_probs[i - 1] + base / pow((double)i, zipf);
          }
        } else {
          logger->info("Using a uniform random distribution.");
        }

        size_t count = 0;
        auto benchmark_start = std::chrono::system_clock::now();
        auto benchmark_end = std::chrono::system_clock::now();
        auto epoch_start = std::chrono::system_clock::now();
        auto epoch_end = std::chrono::system_clock::now();
        auto total_time = std::chrono::duration_cast<std::chrono::seconds>(
                              benchmark_end - benchmark_start)
                              .count();
        unsigned epoch = 1;

        while (true) {
          Key key;
          unsigned k;

          if (zipf > 0) {
            k = sample(num_keys, seed, base, sum_probs);
          } else {
            k = rand_r(&seed) % (num_keys) + 1;
          }

          key = std::string(8 - std::to_string(k).length(), '0') +
                std::to_string(k);
          unsigned trial = 1;

          if (type == "G") {
            handle_request(key, "", pushers, routing_addresses,
                           key_address_cache, seed, logger, ut, response_puller,
                           key_address_puller, ip, thread_id, rid, trial);
            count += 1;
          } else if (type == "P") {
            handle_request(key, std::string(length, 'a'), pushers,
                           routing_addresses, key_address_cache, seed, logger,
                           ut, response_puller, key_address_puller, ip,
                           thread_id, rid, trial);
            count += 1;
          } else if (type == "M") {
            auto req_start = std::chrono::system_clock::now();
            handle_request(key, std::string(length, 'a'), pushers,
                           routing_addresses, key_address_cache, seed, logger,
                           ut, response_puller, key_address_puller, ip,
                           thread_id, rid, trial);
            trial = 1;

            handle_request(key, "", pushers, routing_addresses,
                           key_address_cache, seed, logger, ut, response_puller,
                           key_address_puller, ip, thread_id, rid, trial);
            count += 2;
            auto req_end = std::chrono::system_clock::now();

            double key_latency =
                (double)std::chrono::duration_cast<std::chrono::microseconds>(
                    req_end - req_start)
                    .count() /
                2;

            if (observed_latency.find(key) == observed_latency.end()) {
              observed_latency[key].first = key_latency;
              observed_latency[key].second = 1;
            } else {
              observed_latency[key].first =
                  (observed_latency[key].first * observed_latency[key].second +
                   key_latency) /
                  (observed_latency[key].second + 1);
              observed_latency[key].second += 1;
            }
          } else {
            logger->info("{} is an invalid request type.", type);
          }

          epoch_end = std::chrono::system_clock::now();
          auto time_elapsed = std::chrono::duration_cast<std::chrono::seconds>(
                                  epoch_end - epoch_start)
                                  .count();

          // report throughput every report_period seconds
          if (time_elapsed >= report_period) {
            double throughput = (double)count / (double)time_elapsed;
            logger->info("[Epoch {}] Throughput is {} ops/seconds.", epoch,
                         throughput);
            epoch += 1;

            auto latency = (double)1000000 / throughput;
            UserFeedback feedback;

            feedback.set_uid(ip + ":" + std::to_string(thread_id));
            feedback.set_latency(latency);
            feedback.set_throughput(throughput);

            for (const auto& key_latency_pair : observed_latency) {
              if (key_latency_pair.second.first > 1) {
                UserFeedback_KeyLatency* kl = feedback.add_key_latency();
                kl->set_key(key_latency_pair.first);
                kl->set_latency(key_latency_pair.second.first);
              }
            }

            std::string serialized_latency;
            feedback.SerializeToString(&serialized_latency);

            for (const MonitoringThread& thread : monitoring_threads) {
              kZmqUtil->send_string(
                  serialized_latency,
                  &pushers[thread.get_latency_report_connect_addr()]);
            }

            count = 0;
            observed_latency.clear();
            epoch_start = std::chrono::system_clock::now();
          }

          benchmark_end = std::chrono::system_clock::now();
          total_time = std::chrono::duration_cast<std::chrono::seconds>(
                           benchmark_end - benchmark_start)
                           .count();
          if (total_time > time) {
            break;
          }

          // reset rid
          if (rid > 10000000) {
            rid = 0;
          }
        }

        logger->info("Finished");
        UserFeedback feedback;

        feedback.set_uid(ip + ":" + std::to_string(thread_id));
        feedback.set_finish(true);

        std::string serialized_latency;
        feedback.SerializeToString(&serialized_latency);

        for (const MonitoringThread& thread : monitoring_threads) {
          kZmqUtil->send_string(
              serialized_latency,
              &pushers[thread.get_latency_report_connect_addr()]);
        }
      } else if (mode == "WARM") {
        unsigned num_keys = stoi(v[1]);
        unsigned length = stoi(v[2]);
        unsigned total_threads = stoi(v[3]);
        unsigned range = num_keys / total_threads;
        unsigned start = thread_id * range + 1;
        unsigned end = thread_id * range + 1 + range;

        Key key;
        logger->info("Warming up data");
        auto warmup_start = std::chrono::system_clock::now();

        for (unsigned i = start; i < end; i++) {
          unsigned trial = 1;
          key = std::string(8 - std::to_string(i).length(), '0') +
                std::to_string(i);
          handle_request(key, std::string(length, 'a'), pushers,
                         routing_addresses, key_address_cache, seed, logger, ut,
                         response_puller, key_address_puller, ip, thread_id,
                         rid, trial);

          // reset rid
          if (rid > 10000000) {
            rid = 0;
          }

          if (i == (end - start) / 2) {
            logger->info("Warmed up half.");
          }
        }

        auto warmup_time = std::chrono::duration_cast<std::chrono::seconds>(
                               std::chrono::system_clock::now() - warmup_start)
                               .count();
        logger->info("Warming up data took {} seconds.", warmup_time);
      } else {
        logger->info("{} is an invalid mode.", mode);
      }
    }
  }
}

int main(int argc, char* argv[]) {
  if (argc != 1) {
    std::cerr << "Usage: " << argv[0] << std::endl;
    return 1;
  }

  // read the YAML conf
  YAML::Node conf = YAML::LoadFile("conf/config.yml");
  YAML::Node user = conf["user"];
  std::string ip = user["ip"].as<std::string>();

  std::vector<Address> routing_addresses;
  std::vector<MonitoringThread> monitoring_threads;

  YAML::Node routing = user["routing"];
  YAML::Node monitoring = user["monitoring"];

  for (const YAML::Node& node : monitoring) {
    monitoring_threads.push_back(MonitoringThread(node.as<Address>()));
  }

  for (const YAML::Node& node : routing) {
    routing_addresses.push_back(node.as<Address>());
  }

  YAML::Node threads = conf["threads"];
  kRoutingThreadCount = threads["routing"].as<int>();
  kBenchmarkThreadNum = threads["benchmark"].as<int>();
  kDefaultLocalReplication = conf["replication"]["local"].as<unsigned>();

  std::vector<std::thread> benchmark_threads;
  for (unsigned thread_id = 1; thread_id < kBenchmarkThreadNum; thread_id++) {
    benchmark_threads.push_back(
        std::thread(run, thread_id, ip, routing_addresses, monitoring_threads));
  }

  run(0, ip, routing_addresses, monitoring_threads);
}
