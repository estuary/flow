#!/bin/bash

set -o errexit
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

  cat ${ROOT_DIR}/supabase/tests/*.test.sql

  cat<<EOF


\set QUIET 0
select * from runtests('tests'::name);
\set QUIET 1

drop extension pgtap;
rollback;
EOF

}

psql_input | psql postgres://postgres:postgres@localhost:5432/postgres
