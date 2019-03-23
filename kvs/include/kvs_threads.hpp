
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

#ifndef KVS_INCLUDE_THREADS_HPP_
#define KVS_INCLUDE_THREADS_HPP_

#include "threads.hpp"
#include "types.hpp"

// define server base ports
const unsigned kNodeJoinPort = 6000;
const unsigned kNodeDepartPort = 6050;
const unsigned kSelfDepartPort = 6100;
const unsigned kServerReplicationResponsePort = 6150;
const unsigned kKeyRequestPort = 6200;
const unsigned kGossipPort = 6250;
const unsigned kServerReplicationChangePort = 6300;
const unsigned kCacheIpResponsePort = 7050;

// define routing base ports
const unsigned kSeedPort = 6350;
const unsigned kRoutingNotifyPort = 6400;
const unsigned kRoutingReplicationResponsePort = 6500;
const unsigned kRoutingReplicationChangePort = 6550;

// used by monitoring nodes
const unsigned kMonitoringNotifyPort = 6600;
const unsigned kMonitoringResponsePort = 6650;
const unsigned kDepartDonePort = 6700;
const unsigned kLatencyReportPort = 6750;

class ServerThread {
  Address public_ip_;
  Address public_base_;

  Address private_ip_;
  Address private_base_;

  unsigned tid_;
  unsigned virtual_num_;

 public:
  ServerThread() {}
  ServerThread(Address public_ip, Address private_ip, unsigned tid) :
      public_ip_(public_ip),
      private_ip_(private_ip),
      private_base_("tcp://" + private_ip_ + ":"),
      public_base_("tcp://" + public_ip_ + ":"),
      tid_(tid) {}

  ServerThread(Address public_ip, Address private_ip, unsigned tid,
               unsigned virtual_num) :
      public_ip_(public_ip),
      private_ip_(private_ip),
      private_base_("tcp://" + private_ip_ + ":"),
      public_base_("tcp://" + public_ip_ + ":"),
      tid_(tid),
      virtual_num_(virtual_num) {}

  Address public_ip() const { return public_ip_; }

  Address private_ip() const { return private_ip_; }

  unsigned tid() const { return tid_; }

  unsigned virtual_num() const { return virtual_num_; }

  string id() const { return private_ip_ + ":" + std::to_string(tid_); }

  string virtual_id() const {
    return private_ip_ + ":" + std::to_string(tid_) + "_" +
           std::to_string(virtual_num_);
  }

  Address node_join_connect_address() const {
    return private_base_ + std::to_string(tid_ + kNodeJoinPort);
  }

  Address node_join_bind_address() const {
    return kBindBase + std::to_string(tid_ + kNodeJoinPort);
  }

  Address node_depart_connect_address() const {
    return private_base_ + std::to_string(tid_ + kNodeDepartPort);
  }

  Address node_depart_bind_address() const {
    return kBindBase + std::to_string(tid_ + kNodeDepartPort);
  }

  Address self_depart_connect_address() const {
    return private_base_ + std::to_string(tid_ + kSelfDepartPort);
  }

  Address self_depart_bind_address() const {
    return kBindBase + std::to_string(tid_ + kSelfDepartPort);
  }

  Address key_request_connect_address() const {
    return public_base_ + std::to_string(tid_ + kKeyRequestPort);
  }

  Address key_request_bind_address() const {
    return kBindBase + std::to_string(tid_ + kKeyRequestPort);
  }

  Address replication_response_connect_address() const {
    return private_base_ +
           std::to_string(tid_ + kServerReplicationResponsePort);
  }

  Address replication_response_bind_address() const {
    return kBindBase + std::to_string(tid_ + kServerReplicationResponsePort);
  }

  Address cache_ip_response_connect_address() const {
    return private_base_ + std::to_string(tid_ + kCacheIpResponsePort);
  }

  Address cache_ip_response_bind_address() const {
    return kBindBase + std::to_string(tid_ + kCacheIpResponsePort);
  }

  Address gossip_connect_address() const {
    return private_base_ + std::to_string(tid_ + kGossipPort);
  }

  Address gossip_bind_address() const {
    return kBindBase + std::to_string(tid_ + kGossipPort);
  }

  Address replication_change_connect_address() const {
    return private_base_ + std::to_string(tid_ + kServerReplicationChangePort);
  }

  Address replication_change_bind_address() const {
    return kBindBase + std::to_string(tid_ + kServerReplicationChangePort);
  }
};

inline bool operator==(const ServerThread& l, const ServerThread& r) {
  if (l.id().compare(r.id()) == 0) {
    return true;
  } else {
    return false;
  }
}

class RoutingThread {
  Address ip_;
  Address ip_base_;
  unsigned tid_;

 public:
  RoutingThread() {}

  RoutingThread(Address ip, unsigned tid) :
      ip_(ip),
      tid_(tid),
      ip_base_("tcp://" + ip_ + ":") {}

  Address ip() const { return ip_; }

  unsigned tid() const { return tid_; }

  Address seed_connect_address() const {
    return ip_base_ + std::to_string(tid_ + kSeedPort);
  }

  Address seed_bind_address() const {
    return kBindBase + std::to_string(tid_ + kSeedPort);
  }

  Address notify_connect_address() const {
    return ip_base_ + std::to_string(tid_ + kRoutingNotifyPort);
  }

  Address notify_bind_address() const {
    return kBindBase + std::to_string(tid_ + kRoutingNotifyPort);
  }

  Address key_address_connect_address() const {
    return ip_base_ + std::to_string(tid_ + kKeyAddressPort);
  }

  Address key_address_bind_address() const {
    return kBindBase + std::to_string(tid_ + kKeyAddressPort);
  }

  Address replication_response_connect_address() const {
    return ip_base_ + std::to_string(tid_ + kRoutingReplicationResponsePort);
  }

  Address replication_response_bind_address() const {
    return kBindBase + std::to_string(tid_ + kRoutingReplicationResponsePort);
  }

  Address replication_change_connect_address() const {
    return ip_base_ + std::to_string(tid_ + kRoutingReplicationChangePort);
  }

  Address replication_change_bind_address() const {
    return kBindBase + std::to_string(tid_ + kRoutingReplicationChangePort);
  }
};

class MonitoringThread {
  Address ip_;
  Address ip_base_;

 public:
  MonitoringThread() {}
  MonitoringThread(Address ip) : ip_(ip), ip_base_("tcp://" + ip_ + ":") {}

  Address ip() const { return ip_; }

  Address notify_connect_address() const {
    return ip_base_ + std::to_string(kMonitoringNotifyPort);
  }

  Address notify_bind_address() const {
    return kBindBase + std::to_string(kMonitoringNotifyPort);
  }

  Address response_connect_address() const {
    return ip_base_ + std::to_string(kMonitoringResponsePort);
  }

  Address response_bind_address() const {
    return kBindBase + std::to_string(kMonitoringResponsePort);
  }

  Address depart_done_connect_address() const {
    return ip_base_ + std::to_string(kDepartDonePort);
  }

  Address depart_done_bind_address() const {
    return kBindBase + std::to_string(kDepartDonePort);
  }

  Address latency_report_connect_address() const {
    return ip_base_ + std::to_string(kLatencyReportPort);
  }

  Address latency_report_bind_address() const {
    return kBindBase + std::to_string(kLatencyReportPort);
  }
};

class BenchmarkThread {
 public:
  BenchmarkThread() {}
  BenchmarkThread(Address ip, unsigned tid) : ip_(ip), tid_(tid) {}

  Address ip() const { return ip_; }

  unsigned tid() const { return tid_; }

  Address benchmark_command_address() const {
    return "tcp://" + ip_ + ":" + std::to_string(tid_ + kBenchmarkCommandPort);
  }

 private:
  Address ip_;
  unsigned tid_;
};

inline string get_join_count_req_address(string management_ip) {
  return "tcp://" + management_ip + ":" + std::to_string(kKopsRestartCountPort);
}

struct ThreadHash {
  std::size_t operator()(const ServerThread& st) const {
    return std::hash<string>{}(st.id());
  }
};
#endif  // KVS_INCLUDE_THREADS_HPP_
