#!/bin/bash

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

SCRIPTS=("scripts/check-clang.sh" "kvs/tests/simple/test-simple.sh" "kvs/tests/simple/test-simple-async.sh" "scripts/travis/run-tests.sh")

./scripts/build-all.sh -bDebug -t
EXIT=$?
if [[ $EXIT -ne 0 ]]; then
  echo "$SCRIPT failed with exit code $EXIT."
  exit $EXIT
fi

for SCRIPT in "${SCRIPTS[@]}"; do
  ./"$SCRIPT"
  EXIT=$?
  if [[ $EXIT -ne 0 ]]; then
    echo "$SCRIPT failed with exit code $EXIT."
    exit $EXIT
  fi
done

exit 0
