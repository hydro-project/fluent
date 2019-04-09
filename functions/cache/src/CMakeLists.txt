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

SET(FUNC_CACHE_SOURCE 
	cache.cpp
  )

ADD_EXECUTABLE(flfunc-cache ${FUNC_CACHE_SOURCE})
TARGET_LINK_LIBRARIES(flfunc-cache ${FUNC_DEPENDENCIES})
ADD_DEPENDENCIES(flfunc-cache flzmq zeromq zeromqcpp)

SET(FUNC_ASYNC_CACHE_SOURCE 
	async_cache.cpp
  )

ADD_EXECUTABLE(flfunc-async-cache ${FUNC_ASYNC_CACHE_SOURCE})
TARGET_LINK_LIBRARIES(flfunc-async-cache ${FUNC_DEPENDENCIES})
ADD_DEPENDENCIES(flfunc-async-cache flzmq zeromq zeromqcpp)

SET(FUNC_CAUSAL_CACHE_SOURCE 
	causal_cache.cpp
	causal_cache_utils.cpp
	causal_cache_get_request_handler.cpp
	causal_cache_put_request_handler.cpp
	causal_cache_versioned_key_handlers.cpp
	causal_cache_kvs_response_handler.cpp
	causal_cache_periodic_migration_handler.cpp
  )

ADD_EXECUTABLE(flfunc-causal-cache ${FUNC_CAUSAL_CACHE_SOURCE})
TARGET_LINK_LIBRARIES(flfunc-causal-cache ${FUNC_DEPENDENCIES})
ADD_DEPENDENCIES(flfunc-causal-cache flzmq zeromq zeromqcpp)
