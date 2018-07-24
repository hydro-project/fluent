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

#ifndef SRC_INCLUDE_THREADS_HPP_
#define SRC_INCLUDE_THREADS_HPP_

#include "types.hpp"

// define server base ports
const unsigned kNodeJoinBasePort = 6000;
const unsigned kNodeDepartBasePort = 6050;
const unsigned kSelfDepartBasePort = 6100;
const unsigned kServerReplicationFactorBasePort = 6150;
const unsigned kServerRequestPullingBasePort = 6200;
const unsigned kGossipBasePort = 6250;
const unsigned kServerReplicationFactorChangeBasePort = 6300;

// define routing base ports
const unsigned kSeedBasePort = 6350;
const unsigned kRoutingNotifyBasePort = 6400;
const unsigned kRoutingKeyAddressBasePort = 6450;
const unsigned kRoutingReplicationFactorBasePort = 6500;
const unsigned kRoutingReplicationFactorChangeBasePort = 6550;

// used by monitoring nodes
const unsigned kMonitoringNotifyBasePort = 6600;
const unsigned kMonitoringRequestPullingBasePort = 6650;
const unsigned kDepartDoneBasePort = 6700;
const unsigned kLatencyReportBasePort = 6750;

// used by user nodes
const unsigned kUserRequestBasePort = 6800;
const unsigned kUserKeyAddressBasePort = 6850;
const unsigned kBenchmarkCommandBasePort = 6900;

// TODO(vikram): All the return "tcp://" + ip_ should be made into one command
// to reduce string addition
class ServerThread {
  Address ip_;
  unsigned tid_;
  unsigned virtual_num_;

 public:
  ServerThread() {}
  ServerThread(Address ip, unsigned tid) : ip_(ip), tid_(tid) {}
  ServerThread(Address ip, unsigned tid, unsigned virtual_num) :
      ip_(ip),
      tid_(tid),
      virtual_num_(virtual_num) {}

  Address get_ip() const { return ip_; }
  unsigned get_tid() const { return tid_; }
  unsigned get_virtual_num() const { return virtual_num_; }
  std::string get_id() const { return ip_ + ":" + std::to_string(tid_); }
  std::string get_virtual_id() const {
    return ip_ + ":" + std::to_string(tid_) + "_" +
           std::to_string(virtual_num_);
  }
  Address get_node_join_connect_addr() const {
    return "tcp://" + ip_ + ":" + std::to_string(tid_ + kNodeJoinBasePort);
  }
  Address get_node_join_bind_addr() const {
    return "tcp://*:" + std::to_string(tid_ + kNodeJoinBasePort);
  }
  Address get_node_depart_connect_addr() const {
    return "tcp://" + ip_ + ":" + std::to_string(tid_ + kNodeDepartBasePort);
  }
  Address get_node_depart_bind_addr() const {
    return "tcp://*:" + std::to_string(tid_ + kNodeDepartBasePort);
  }
  Address get_self_depart_connect_addr() const {
    return "tcp://" + ip_ + ":" + std::to_string(tid_ + kSelfDepartBasePort);
  }
  Address get_self_depart_bind_addr() const {
    return "tcp://*:" + std::to_string(tid_ + kSelfDepartBasePort);
  }
  Address get_request_pulling_connect_addr() const {
    return "tcp://" + ip_ + ":" +
           std::to_string(tid_ + kServerRequestPullingBasePort);
  }
  Address get_request_pulling_bind_addr() const {
    return "tcp://*:" + std::to_string(tid_ + kServerRequestPullingBasePort);
  }
  Address get_replication_factor_connect_addr() const {
    return "tcp://" + ip_ + ":" +
           std::to_string(tid_ + kServerReplicationFactorBasePort);
  }
  Address get_replication_factor_bind_addr() const {
    return "tcp://*:" + std::to_string(tid_ + kServerReplicationFactorBasePort);
  }
  Address get_gossip_connect_addr() const {
    return "tcp://" + ip_ + ":" + std::to_string(tid_ + kGossipBasePort);
  }
  Address get_gossip_bind_addr() const {
    return "tcp://*:" + std::to_string(tid_ + kGossipBasePort);
  }
  Address get_replication_factor_change_connect_addr() const {
    return "tcp://" + ip_ + ":" +
           std::to_string(tid_ + kServerReplicationFactorChangeBasePort);
  }
  Address get_replication_factor_change_bind_addr() const {
    return "tcp://*:" +
           std::to_string(tid_ + kServerReplicationFactorChangeBasePort);
  }
};

inline bool operator==(const ServerThread& l, const ServerThread& r) {
  if (l.get_id().compare(r.get_id()) == 0) {
    return true;
  } else {
    return false;
  }
}

class RoutingThread {
  Address ip_;
  unsigned tid_;

 public:
  RoutingThread() {}
  RoutingThread(Address ip, unsigned tid) : ip_(ip), tid_(tid) {}

  Address get_ip() const { return ip_; }

  unsigned get_tid() const { return tid_; }

  Address get_seed_connect_addr() const {
    return "tcp://" + ip_ + ":" + std::to_string(tid_ + kSeedBasePort);
  }

  Address get_seed_bind_addr() const {
    return "tcp://*:" + std::to_string(tid_ + kSeedBasePort);
  }

  Address get_notify_connect_addr() const {
    return "tcp://" + ip_ + ":" + std::to_string(tid_ + kRoutingNotifyBasePort);
  }

  Address get_notify_bind_addr() const {
    return "tcp://*:" + std::to_string(tid_ + kRoutingNotifyBasePort);
  }

  Address get_key_address_connect_addr() const {
    return "tcp://" + ip_ + ":" +
           std::to_string(tid_ + kRoutingKeyAddressBasePort);
  }

  Address get_key_address_bind_addr() const {
    return "tcp://*:" + std::to_string(tid_ + kRoutingKeyAddressBasePort);
  }

  Address get_replication_factor_connect_addr() const {
    return "tcp://" + ip_ + ":" +
           std::to_string(tid_ + kRoutingReplicationFactorBasePort);
  }

  Address get_replication_factor_bind_addr() const {
    return "tcp://*:" +
           std::to_string(tid_ + kRoutingReplicationFactorBasePort);
  }

  Address get_replication_factor_change_connect_addr() const {
    return "tcp://" + ip_ + ":" +
           std::to_string(tid_ + kRoutingReplicationFactorChangeBasePort);
  }

  Address get_replication_factor_change_bind_addr() const {
    return "tcp://*:" +
           std::to_string(tid_ + kRoutingReplicationFactorChangeBasePort);
  }
};

class MonitoringThread {
  Address ip_;

 public:
  MonitoringThread() {}
  MonitoringThread(Address ip) : ip_(ip) {}

  Address get_ip() const { return ip_; }

  Address get_notify_connect_addr() const {
    return "tcp://" + ip_ + ":" + std::to_string(kMonitoringNotifyBasePort);
  }

  Address get_notify_bind_addr() const {
    return "tcp://*:" + std::to_string(kMonitoringNotifyBasePort);
  }

  Address get_request_pulling_connect_addr() const {
    return "tcp://" + ip_ + ":" +
           std::to_string(kMonitoringRequestPullingBasePort);
  }

  Address get_request_pulling_bind_addr() const {
    return "tcp://*:" + std::to_string(kMonitoringRequestPullingBasePort);
  }

  Address get_depart_done_connect_addr() const {
    return "tcp://" + ip_ + ":" + std::to_string(kDepartDoneBasePort);
  }

  Address get_depart_done_bind_addr() const {
    return "tcp://*:" + std::to_string(kDepartDoneBasePort);
  }

  Address get_latency_report_connect_addr() const {
    return "tcp://" + ip_ + ":" + std::to_string(kLatencyReportBasePort);
  }

  Address get_latency_report_bind_addr() const {
    return "tcp://*:" + std::to_string(kLatencyReportBasePort);
  }
};

class UserThread {
  Address ip_;
  unsigned tid_;

 public:
  UserThread() {}
  UserThread(Address ip, unsigned tid) : ip_(ip), tid_(tid) {}

  Address get_ip() const { return ip_; }

  unsigned get_tid() const { return tid_; }

  Address get_request_pulling_connect_addr() const {
    return "tcp://" + ip_ + ":" + std::to_string(tid_ + kUserRequestBasePort);
  }

  Address get_request_pulling_bind_addr() const {
    return "tcp://*:" + std::to_string(tid_ + kUserRequestBasePort);
  }

  Address get_key_address_connect_addr() const {
    return "tcp://" + ip_ + ":" +
           std::to_string(tid_ + kUserKeyAddressBasePort);
  }

  Address get_key_address_bind_addr() const {
    return "tcp://*:" + std::to_string(tid_ + kUserKeyAddressBasePort);
  }
};

class BenchmarkThread {
 public:
  BenchmarkThread() {}
  BenchmarkThread(Address ip, unsigned tid) : ip_(ip), tid_(tid) {}

  Address get_ip() const { return ip_; }

  unsigned get_tid() const { return tid_; }

  Address get_benchmark_command_port_addr() const {
    return "tcp://" + ip_ + ":" +
           std::to_string(tid_ + kBenchmarkCommandBasePort);
  }

 private:
  Address ip_;
  unsigned tid_;
};

#endif  // SRC_INCLUDE_THREADS_HPP_
