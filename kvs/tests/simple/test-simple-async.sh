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

if [ $# -gt 2 ]; then
  echo "Usage: $0 <build>"
  echo "If no build option is specified, the test will default to not building."

  exit 1
fi

if [ -z "$1" ]; then
  BUILD="n"
else
  BUILD=$1
fi

echo "Starting local server..."
./scripts/start-kvs-local.sh $BUILD n

echo "Running tests..."
./build/kvs/client/cpp/flkvs-async-cli conf/kvs-example.yml kvs/tests/simple/input_async > tmp.out

DIFF=`diff tmp.out kvs/tests/simple/expected_async`

if [ "$DIFF" != "" ]; then
  echo "Output did not match expected output (tests/simple/expected.out). Observed output was: "
  echo $DIFF
  CODE=1
else
  echo "Test succeeded!"
  CODE=0
fi

rm tmp.out
echo "Stopping local server..."
./scripts/stop-kvs-local.sh y
exit $CODE
