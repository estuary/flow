#!/bin/sh

sqlite3 ":memory:" < catalog_test.sql 2>&1 | \
	diff - catalog_test_golden.out \
		--report-identical-files \
		--context=3

