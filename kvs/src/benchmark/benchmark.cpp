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

#include "kvs_client.hpp"
#include "kvs_threads.hpp"
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
           map<unsigned, double>& sum_probs) {
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

string generate_key(unsigned n) {
  return string(8 - std::to_string(n).length(), '0') + std::to_string(n);
}

void run(const unsigned& thread_id,
         const vector<UserRoutingThread>& routing_threads,
         const vector<MonitoringThread>& monitoring_threads,
         const Address& ip) {
  KvsClient client(routing_threads, ip, thread_id, 10000);
  string log_file = "log_" + std::to_string(thread_id) + ".txt";
  string logger_name = "benchmark_log_" + std::to_string(thread_id);
  auto log = spdlog::basic_logger_mt(logger_name, log_file, true);
  log->flush_on(spdlog::level::info);

  client.set_logger(log);
  unsigned seed = client.get_seed();

  // observed per-key avg latency
  map<Key, std::pair<double, unsigned>> observed_latency;

  // responsible for pulling benchmark commands
  zmq::context_t& context = *(client.get_context());
  SocketCache pushers(&context, ZMQ_PUSH);
  zmq::socket_t command_puller(context, ZMQ_PULL);
  command_puller.bind("tcp://*:" +
                      std::to_string(thread_id + kBenchmarkCommandPort));

  vector<zmq::pollitem_t> pollitems = {
      {static_cast<void*>(command_puller), 0, ZMQ_POLLIN, 0}};

  while (true) {
    kZmqUtil->poll(-1, &pollitems);

    if (pollitems[0].revents & ZMQ_POLLIN) {
      string msg = kZmqUtil->recv_string(&command_puller);
      log->info("Received benchmark command: {}", msg);

      vector<string> v;
      split(msg, ':', v);
      string mode = v[0];

      if (mode == "CACHE") {
        unsigned num_keys = stoi(v[1]);
        // warm up cache
        client.clear_cache();
        auto warmup_start = std::chrono::system_clock::now();

        for (unsigned i = 1; i <= num_keys; i++) {
          if (i % 50000 == 0) {
            log->info("Warming up cache for key {}.", i);
          }

          client.get(generate_key(i));
        }

        auto warmup_time = std::chrono::duration_cast<std::chrono::seconds>(
                               std::chrono::system_clock::now() - warmup_start)
                               .count();
        log->info("Cache warm-up took {} seconds.", warmup_time);
      } else if (mode == "LOAD") {
        string type = v[1];
        unsigned num_keys = stoi(v[2]);
        unsigned length = stoi(v[3]);
        unsigned report_period = stoi(v[4]);
        unsigned time = stoi(v[5]);
        double zipf = stod(v[6]);

        map<unsigned, double> sum_probs;
        double base;

        if (zipf > 0) {
          log->info("Zipf coefficient is {}.", zipf);
          base = get_base(num_keys, zipf);
          sum_probs[0] = 0;

          for (unsigned i = 1; i <= num_keys; i++) {
            sum_probs[i] = sum_probs[i - 1] + base / pow((double)i, zipf);
          }
        } else {
          log->info("Using a uniform random distribution.");
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
            unsigned ts = generate_timestamp(thread_id);
            LWWPairLattice<string> val(
                TimestampValuePair<string>(ts, string(length, 'a')));

            client.put(key, val);
            count += 1;
          } else if (type == "M") {
            auto req_start = std::chrono::system_clock::now();
            unsigned ts = generate_timestamp(thread_id);
            LWWPairLattice<string> val(
                TimestampValuePair<string>(ts, string(length, 'a')));

            client.put(key, val);
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
            log->info("{} is an invalid request type.", type);
          }

          epoch_end = std::chrono::system_clock::now();
          auto time_elapsed = std::chrono::duration_cast<std::chrono::seconds>(
                                  epoch_end - epoch_start)
                                  .count();

          // report throughput every report_period seconds
          if (time_elapsed >= report_period) {
            double throughput = (double)count / (double)time_elapsed;
            log->info("[Epoch {}] Throughput is {} ops/seconds.", epoch,
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

            string serialized_latency;
            feedback.SerializeToString(&serialized_latency);

            for (const MonitoringThread& thread : monitoring_threads) {
              kZmqUtil->send_string(
                  serialized_latency,
                  &pushers[thread.latency_report_connect_address()]);
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

        log->info("Finished");
        UserFeedback feedback;

        feedback.set_uid(ip + ":" + std::to_string(thread_id));
        feedback.set_finish(true);

        string serialized_latency;
        feedback.SerializeToString(&serialized_latency);

        for (const MonitoringThread& thread : monitoring_threads) {
          kZmqUtil->send_string(
              serialized_latency,
              &pushers[thread.latency_report_connect_address()]);
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
            log->info("Creating key {}.", i);
          }

          unsigned ts = generate_timestamp(thread_id);
          LWWPairLattice<string> val(
              TimestampValuePair<string>(ts, string(length, 'a')));
          client.put(generate_key(i), val);
        }

        auto warmup_time = std::chrono::duration_cast<std::chrono::seconds>(
                               std::chrono::system_clock::now() - warmup_start)
                               .count();
        log->info("Warming up data took {} seconds.", warmup_time);
      } else {
        log->info("{} is an invalid mode.", mode);
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
  YAML::Node conf = YAML::LoadFile("conf/kvs-config.yml");
  YAML::Node user = conf["user"];
  Address ip = user["ip"].as<string>();

  vector<MonitoringThread> monitoring_threads;
  vector<Address> routing_ips;

  YAML::Node monitoring = user["monitoring"];
  for (const YAML::Node& node : monitoring) {
    monitoring_threads.push_back(MonitoringThread(node.as<Address>()));
  }

  YAML::Node threads = conf["threads"];
  kRoutingThreadCount = threads["routing"].as<int>();
  kBenchmarkThreadNum = threads["benchmark"].as<int>();
  kDefaultLocalReplication = conf["replication"]["local"].as<unsigned>();

  vector<std::thread> benchmark_threads;

  if (YAML::Node elb = user["routing-elb"]) {
    routing_ips.push_back(elb.as<string>());
  } else {
    YAML::Node routing = user["routing"];

    for (const YAML::Node& node : routing) {
      routing_ips.push_back(node.as<Address>());
    }
  }

  vector<UserRoutingThread> routing_threads;
  for (const Address& ip : routing_ips) {
    for (unsigned i = 0; i < kRoutingThreadCount; i++) {
      routing_threads.push_back(UserRoutingThread(ip, i));
    }
  }

  // NOTE: We create a new client for every single thread.
  for (unsigned thread_id = 1; thread_id < kBenchmarkThreadNum; thread_id++) {
    benchmark_threads.push_back(
        std::thread(run, thread_id, routing_threads, monitoring_threads, ip));
  }

  run(0, routing_threads, monitoring_threads, ip);
}
