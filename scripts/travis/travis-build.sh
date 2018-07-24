#!/bin/bash

SCRIPTS=("scripts/check-clang.sh" "kvs/tests/simple/test-simple.sh" "scripts/travis/run-tests.sh")

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
