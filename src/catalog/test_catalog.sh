#!/bin/bash

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
OUT=$(mktemp -t test_catalog-XXXXXXX)

# Function which produces sqlite3 CLI meta-commands to load the PCRE
# module, sets up the catalog schema, re-configures to emit output
# for catalog test, and then produces tests themselves.
function test_input() {

	cat <<EOF
.load /usr/lib/sqlite3/pcre.so
EOF

	# Process schema itself.
	cat ${DIR}/catalog.sql

	# Reconfigure for tests.
	cat <<EOF
.changes on
.headers on
.echo on
EOF
	# Process tests.
	cat ${DIR}/catalog_test.sql
}


# Feed test_output() into an empty in-memory database. 
# Strip error line numbers so differences in output are localized (and
# don't require updating the entire golden file when the change).
test_input | \
	sqlite3 ":memory:" 2>&1 | \
	sed --regexp-extended 's/Error: near line [[:digit:]]+:/Error: near line (XYZ):/' | \
	sed --regexp-extended 's/total_changes: [[:digit:]]+/total_changes: XYZ/' > ${OUT}

# Compare test output with checked-in "golden" version.
diff ${OUT} ${DIR}/catalog_test_golden.out \
	--report-identical-files \
	--context=3

# Remove temp file only if test passed.
if [ $? -eq 0 ]; then
	rm ${OUT}
fi
