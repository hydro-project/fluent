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

#!/bin/bash

if [ -z "$1" ]; then
  echo "Usage: ./scripts/stop_local.sh remove-logs"
  exit 1
fi

while IFS='' read -r line || [[ -n "$line" ]] ; do
  kill $line
done < "pids"

if [ "$1" = "y" ]; then
  rm log*
fi

rm pids
