CREATE TABLE foo (
	id INTEGER NOT NULL primary key,
	name text
);
	
CREATE INDEX fooIdx ON foo (name);

BEGIN;
INSERT INTO foo (id, name) VALUES (0, 'hello, sqlite 000');
INSERT INTO foo (id, name) VALUES (1, 'hello, sqlite 001');
INSERT INTO foo (id, name) VALUES (2, 'hello, sqlite 002');
INSERT INTO foo (id, name) VALUES (3, 'hello, sqlite 003');
INSERT INTO foo (id, name) VALUES (4, 'hello, sqlite 004');
INSERT INTO foo (id, name) VALUES (5, 'hello, sqlite 005');
COMMIT;

BEGIN;
INSERT INTO foo (id, name) VALUES (99, 'this is dropped');
INSERT INTO foo (id, name) VALUES (999, 'so is this');
CRASH_AND_RECOVER;

INSERT INTO foo (id, name) VALUES (6, 'hello, sqlite 006');
INSERT INTO foo (id, name) VALUES (7, 'hello, sqlite 007');
INSERT INTO foo (id, name) VALUES (8, 'hello, sqlite 008');

SELECT id, name FROM foo ORDER BY id DESC;
