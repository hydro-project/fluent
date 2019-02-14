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

#include "client.hpp"
#include "spdlog/spdlog.h"
#include "yaml-cpp/yaml.h"

unsigned kBenchmarkThreadNum;
unsigned kRoutingThreadCount;
unsigned kDefaultLocalReplication;

ZmqUtil zmq_util;
ZmqUtilInterface* kZmqUtil = &zmq_util;

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

void run(unsigned thread_id, KvsClient client,
         std::vector<MonitoringThread> monitoring_threads) {
  std::string log_file = "log_" + std::to_string(thread_id) + ".txt";
  std::string logger_name = "benchmark_log_" + std::to_string(thread_id);
  auto logger = spdlog::basic_logger_mt(logger_name, log_file, true);
  logger->flush_on(spdlog::level::info);

  client.set_logger(logger);

  // responsible for pulling benchmark commands
  zmq::socket_t command_puller(context, ZMQ_PULL);
  command_puller.bind("tcp://*:" +
                      std::to_string(thread_id + kBenchmarkCommandBasePort));

  std::vector<zmq::pollitem_t> pollitems = {
      {static_cast<void*>(command_puller), 0, ZMQ_POLLIN, 0}};

  while (true) {
    kZmqUtil->poll(-1, &pollitems);

    if (pollitems[0].revents & ZMQ_POLLIN) {
      std::string msg = kZmqUtil->recv_string(&command_puller);
      logger->info("Received benchmark command: {}", msg);

      std::vector<std::string> v;
      split(msg, ':', v);
      std::string mode = v[0];

      if (mode == "CACHE") {
        unsigned num_keys = stoi(v[1]);
        // warm up cache
        client.clear_cache();
        auto warmup_start = std::chrono::system_clock::now();

        for (unsigned i = 1; i <= num_keys; i++) {
          if (i % 50000 == 0) {
            logger->info("Warming up cache for key {}.", i);
          }

          client.get(generate_key(i));
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
          unsigned k;
          if (zipf > 0) {
            k = sample(num_keys, seed, base, sum_probs);
          } else {
            k = rand_r(&seed) % (num_keys) + 1;
          }

          Key key = generate_key(k);

          if (type == "G") {
            client.get(key);
            count += 1;
          } else if (type == "P") {
            client.put(key, std::string(length, 'a'));
            count += 1;
          } else if (type == "M") {
            auto req_start = std::chrono::system_clock::now();

            client.put(key, std::string(length, 'a'));
            client.get(key);
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
        auto warmup_start = std::chrono::system_clock::now();

        for (unsigned i = start; i < end; i++) {
          if (i % 50000 == 0) {
            logger->info("Creating key {}.", i);
          }

          client.put(generate_key(i), std::string(length, 'a'));
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

std::string generate_key(unsigned n) {
  return std::string(8 - std::to_string(i).length(), '0') +
                std::to_string(n);
}

int main(int argc, char* argv[]) {
  if (argc != 1) {
    std::cerr << "Usage: " << argv[0] << std::endl;
    return 1;
  }

  // read the YAML conf
  YAML::Node conf = YAML::LoadFile("conf/kvs-config.yml");
  YAML::Node user = conf["user"];
  std::string ip = user["ip"].as<std::string>();

  std::vector<MonitoringThread> monitoring_threads;
  YAML::Node monitoring = user["monitoring"];
  for (const YAML::Node& node : monitoring) {
    monitoring_threads.push_back(MonitoringThread(node.as<Address>()));
  }

  YAML::Node threads = conf["threads"];
  kRoutingThreadCount = threads["routing"].as<int>();
  kBenchmarkThreadNum = threads["benchmark"].as<int>();
  kDefaultLocalReplication = conf["replication"]["local"].as<unsigned>();

  KvsClient client;
  std::vector<std::thread> benchmark_threads;

  if (YAML::Node elb = user["routing-elb"]) {
    for (unsigned thread_id = 1; thread_id < kBenchmarkThreadNum; thread_id++) {
      client = KvsClient(elb.as<std::string>(), kRoutingThreadCount, ip, 0);
      benchmark_threads.push_back(
          std::thread(run, thread_id, client, monitoring_threads));
    }

    client = KvsClient(elb.as<std::string>(), kRoutingThreadCount, ip, 0);
    run(0, client, monitoring_threads);
  } else {
    YAML::Node routing = user["routing"];
    std::vector<Address> routing_addresses;

    for (const YAML::Node& node : routing) {
      routing_addresses.push_back(node.as<Address>());
    }

    for (unsigned thread_id = 1; thread_id < kBenchmarkThreadNum; thread_id++) {
      client = KvsClient(routing_addresses, kRoutingThreadCount, ip, 0);
      benchmark_threads.push_back(
          std::thread(run, thread_id, client, monitoring_threads));
    }

    client = KvsClient(routing_addresses, kRoutingThreadCount, ip, 0);
    run(0, client, monitoring_threads);
  }
}
