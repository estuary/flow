#!/bin/bash

# set -o errexit
set -o pipefail
set -o nounset

ROOT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )/../


function psql_input() {

  cat<<EOF
-- Turn off echo and keep things quiet.
\unset ECHO
\set QUIET 1

-- Format the output for nice TAP.
\pset format unaligned
\pset tuples_only true
\pset pager off

begin;
create extension pgtap;
create schema tests;

EOF

  if [ "$1" -eq 1 ]; then
    cat<<EOF
create function tests.shutdown_exit_on_error()
returns void as \$\$ begin
  ASSERT num_failed() = 0;
end
\$\$ language plpgsql;
EOF
  fi

  cat "${ROOT_DIR}"/supabase/tests/*.test.sql

  cat<<EOF


\set QUIET 0
select * from runtests('tests'::name);
\set QUIET 1

drop extension pgtap;
rollback;
EOF

}

# Cause `psql` to return a non-zero exit code when there's a test failure
# by injecting a shutdown function that checks pgTAP for any errors and
# raises an exception if there is one. Combined with `ON_ERROR_STOP`, this
# will cause `psql` to error.
psql_input 1 | psql --set ON_ERROR_STOP=1 postgres://postgres:postgres@localhost:5432/postgres

test_exit_code=$?

# Normally when pgTAP detects a test failure, it records the failure and prints
# it nicely so you can see what happened. By throwing an exception above, we
# pre-empt that pretty printing in order to detect the failure. Now, if there is
# a failure, let's re-run the tests without the exception to get the pretty output.
if [ $test_exit_code -ne 0 ]; then
  echo "There was a test failure, re-running to show meaningful output";
  psql_input 0 | psql --set ON_ERROR_STOP=1 postgres://postgres:postgres@localhost:5432/postgres;
  # Lastly, make sure to indicate that there was a failure so we can fail the CI run.
  exit 1
fi