#!/bin/bash

set -o pipefail
set -o nounset

ROOT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )/../

# Run all SQL tests by invoking run_single_test.sh for each test file.
# This approach ensures each test runs in its own isolated transaction.

# Find all test files
test_files=("${ROOT_DIR}"/supabase/tests/*.test.sql)

echo "Running ${#test_files[@]} test files..."

failed_tests=()

for test_file in "${test_files[@]}"; do
  test_name=$(basename "$test_file")
  echo "Running: $test_name"

  if ! "${ROOT_DIR}/supabase/run_single_test.sh" "$test_file"; then
    failed_tests+=("$test_name")
    echo "FAILED: $test_name"
  else
    echo "PASSED: $test_name"
  fi
  echo ""
done

# Report results
echo "========================================"
if [ ${#failed_tests[@]} -eq 0 ]; then
  echo "All ${#test_files[@]} tests passed!"
  exit 0
else
  echo "Failed tests (${#failed_tests[@]}/${#test_files[@]}):"
  for test in "${failed_tests[@]}"; do
    echo "  - $test"
  done
  exit 1
fi
