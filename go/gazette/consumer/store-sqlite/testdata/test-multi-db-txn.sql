
ATTACH DATABASE 'ATTACH_DB1' AS db1;
ATTACH DATABASE 'ATTACH_DB2' AS db2;

BEGIN;
CREATE TABLE foo (id INTEGER NOT NULL primary key, name text);
CREATE TABLE db1.foo (id INTEGER NOT NULL primary key, name text);
CREATE TABLE db2.foo (id INTEGER NOT NULL primary key, name text);
COMMIT;

BEGIN;
INSERT INTO foo (id, name) VALUES (0, 'primary DB');
INSERT INTO db1.foo (id, name) VALUES (1, 'db1 value');
INSERT INTO db2.foo (id, name) VALUES (2, 'db2 value');
COMMIT;

SELECT * FROM foo UNION SELECT * FROM db1.foo UNION SELECT * FROM db2.foo;

BEGIN;
INSERT INTO foo (id, name) VALUES (10, 'primary DB two');
INSERT INTO db1.foo (id, name) VALUES (11, 'db1 value two');
INSERT INTO db2.foo (id, name) VALUES (12, 'db2 value two');

CRASH_AND_RECOVER;
ATTACH DATABASE 'ATTACH_DB1' AS db1;
ATTACH DATABASE 'ATTACH_DB2' AS db2;

SELECT * FROM foo UNION SELECT * FROM db1.foo UNION SELECT * FROM db2.foo;

