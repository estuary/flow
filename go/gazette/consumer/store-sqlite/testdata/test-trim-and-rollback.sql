PRAGMA cache_size = 2;
PRAGMA cell_size_check = 1;
PRAGMA journal_mode = TRUNCATE;

CREATE TABLE foo (id INT, name text);
CREATE INDEX fooExpensiveIdx1 ON foo (id, name);
CREATE INDEX fooExpensiveIdx2 ON foo (name, id);

BEGIN;
INSERT INTO foo
	WITH RECURSIVE generator(id, name)
	AS (
		SELECT 1, 1 * 1000
		UNION ALL
		SELECT id+1, (id+1)*100000000 FROM generator
		LIMIT 1000
	)
	SELECT id, name from generator;
COMMIT;

DROP INDEX fooExpensiveIdx1;
DROP INDEX fooExpensiveIdx2;
VACUUM;

BEGIN;
INSERT INTO foo
  SELECT id + 1000, name + "-extra" FROM foo;
ROLLBACK;

SELECT * from foo; 
