#  Copyright 2018 U.C. Berkeley RISE Lab
# 
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
# 
#      http://www.apache.org/licenses/LICENSE-2.0
# 
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.

CMAKE_MINIMUM_REQUIRED(VERSION 3.6 FATAL_ERROR)

SET(MONITORING_SOURCE 
		monitoring.cpp
		membership_handler.cpp
		depart_done_handler.cpp
		feedback_handler.cpp
		metadata_helpers.cpp
		stats_helpers.cpp
		replication_helpers.cpp
		elasticity.cpp
		storage_policy.cpp
		movement_policy.cpp
		slo_policy.cpp)

ADD_EXECUTABLE(flmonitor ${MONITORING_SOURCE})
TARGET_LINK_LIBRARIES(flmonitor flkvs-ring ${KV_LIBRARY_DEPENDENCIES})
ADD_DEPENDENCIES(flmonitor zeromq zeromqcpp)
