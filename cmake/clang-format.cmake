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

# Script copied from https://arcanis.me/en/2015/10/17/cppcheck-and-clang-format

# get list of all project files
FILE(GLOB_RECURSE ALL_SOURCE_FILES *.cpp *.hpp)
foreach (SOURCE_FILE ${ALL_SOURCE_FILES})
  STRING(FIND ${SOURCE_FILE} ${VENDOR_DIR} VENDOR_DIR_FOUND)
    if (NOT ${VENDOR_DIR_FOUND} EQUAL -1)
      LIST(REMOVE_ITEM ALL_SOURCE_FILES ${SOURCE_FILE})
    endif ()
endforeach ()

ADD_CUSTOM_TARGET(
        clang-format
        COMMAND clang-format
        -style=file
        -i
        ${ALL_SOURCE_FILES}
)

