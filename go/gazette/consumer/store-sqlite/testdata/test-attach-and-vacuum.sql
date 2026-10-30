PRAGMA cache_size = 2;
PRAGMA cell_size_check = 1;
	
CREATE TABLE foo (
	id INTEGER NOT NULL primary key,
	name text
);

CREATE INDEX fooIdx ON foo (name);

ATTACH DATABASE 'ATTACH_DB1' AS other;
CREATE TABLE other.bar (
	id INTEGER NOT NULL PRIMARY KEY,
	name text
);

BEGIN;
INSERT INTO foo (id, name) VALUES (0, 'hello, sqlite 000');
INSERT INTO foo (id, name) VALUES (1, 'hello, sqlite 001');
INSERT INTO foo (id, name) VALUES (2, 'hello, sqlite 002');
INSERT INTO foo (id, name) VALUES (3, 'hello, sqlite 003');
INSERT INTO foo (id, name) VALUES (4, 'hello, sqlite 004');
INSERT INTO foo (id, name) VALUES (5, 'hello, sqlite 005');
INSERT INTO foo (id, name) VALUES (6, 'hello, sqlite 006');
INSERT INTO foo (id, name) VALUES (7, 'hello, sqlite 007');
INSERT INTO foo (id, name) VALUES (8, 'hello, sqlite 008');
INSERT INTO foo (id, name) VALUES (9, 'hello, sqlite 009');
INSERT INTO foo (id, name) VALUES (10, 'hello, sqlite 010');
INSERT INTO foo (id, name) VALUES (11, 'hello, sqlite 011');
INSERT INTO foo (id, name) VALUES (12, 'hello, sqlite 012');
INSERT INTO foo (id, name) VALUES (13, 'hello, sqlite 013');
INSERT INTO foo (id, name) VALUES (14, 'hello, sqlite 014');
INSERT INTO foo (id, name) VALUES (15, 'hello, sqlite 015');
INSERT INTO foo (id, name) VALUES (16, 'hello, sqlite 016');
INSERT INTO foo (id, name) VALUES (17, 'hello, sqlite 017');
INSERT INTO foo (id, name) VALUES (18, 'hello, sqlite 018');
INSERT INTO foo (id, name) VALUES (19, 'hello, sqlite 019');
INSERT INTO foo (id, name) VALUES (20, 'hello, sqlite 020');
INSERT INTO foo (id, name) VALUES (21, 'hello, sqlite 021');
INSERT INTO foo (id, name) VALUES (22, 'hello, sqlite 022');
INSERT INTO foo (id, name) VALUES (23, 'hello, sqlite 023');
INSERT INTO foo (id, name) VALUES (24, 'hello, sqlite 024');
INSERT INTO foo (id, name) VALUES (25, 'hello, sqlite 025');
INSERT INTO foo (id, name) VALUES (26, 'hello, sqlite 026');
INSERT INTO foo (id, name) VALUES (27, 'hello, sqlite 027');
COMMIT;

INSERT INTO other.bar SELECT id, name FROM foo WHERE id > 10;

BEGIN;
INSERT INTO foo (id, name) VALUES (28, 'hello, sqlite 028');
INSERT INTO foo (id, name) VALUES (29, 'hello, sqlite 029');
INSERT INTO foo (id, name) VALUES (30, 'hello, sqlite 030');
INSERT INTO foo (id, name) VALUES (31, 'hello, sqlite 031');
INSERT INTO foo (id, name) VALUES (32, 'hello, sqlite 032');
INSERT INTO foo (id, name) VALUES (33, 'hello, sqlite 033');
INSERT INTO foo (id, name) VALUES (34, 'hello, sqlite 034');
INSERT INTO foo (id, name) VALUES (35, 'hello, sqlite 035');
INSERT INTO foo (id, name) VALUES (36, 'hello, sqlite 036');
INSERT INTO foo (id, name) VALUES (37, 'hello, sqlite 037');
INSERT INTO foo (id, name) VALUES (38, 'hello, sqlite 038');
INSERT INTO foo (id, name) VALUES (39, 'hello, sqlite 039');
INSERT INTO foo (id, name) VALUES (40, 'hello, sqlite 040');
INSERT INTO foo (id, name) VALUES (41, 'hello, sqlite 041');
INSERT INTO foo (id, name) VALUES (42, 'hello, sqlite 042');
INSERT INTO foo (id, name) VALUES (43, 'hello, sqlite 043');
INSERT INTO foo (id, name) VALUES (44, 'hello, sqlite 044');
INSERT INTO foo (id, name) VALUES (45, 'hello, sqlite 045');
INSERT INTO foo (id, name) VALUES (46, 'hello, sqlite 046');
INSERT INTO foo (id, name) VALUES (47, 'hello, sqlite 047');
COMMIT;

CRASH_AND_RECOVER;
ATTACH DATABASE 'ATTACH_DB1' AS other;

BEGIN;
INSERT INTO foo (id, name) VALUES (48, 'hello, sqlite 048');
INSERT INTO foo (id, name) VALUES (49, 'hello, sqlite 049');
INSERT INTO foo (id, name) VALUES (50, 'hello, sqlite 050');
INSERT INTO foo (id, name) VALUES (51, 'hello, sqlite 051');
INSERT INTO foo (id, name) VALUES (52, 'hello, sqlite 052');
INSERT INTO foo (id, name) VALUES (53, 'hello, sqlite 053');
INSERT INTO foo (id, name) VALUES (54, 'hello, sqlite 054');
INSERT INTO foo (id, name) VALUES (55, 'hello, sqlite 055');
INSERT INTO foo (id, name) VALUES (56, 'hello, sqlite 056');
INSERT INTO foo (id, name) VALUES (57, 'hello, sqlite 057');
INSERT INTO foo (id, name) VALUES (58, 'hello, sqlite 058');
INSERT INTO foo (id, name) VALUES (59, 'hello, sqlite 059');
INSERT INTO foo (id, name) VALUES (60, 'hello, sqlite 060');
INSERT INTO foo (id, name) VALUES (61, 'hello, sqlite 061');
INSERT INTO foo (id, name) VALUES (62, 'hello, sqlite 062');
INSERT INTO foo (id, name) VALUES (63, 'hello, sqlite 063');
INSERT INTO foo (id, name) VALUES (64, 'hello, sqlite 064');
INSERT INTO foo (id, name) VALUES (65, 'hello, sqlite 065');
INSERT INTO foo (id, name) VALUES (66, 'hello, sqlite 066');
INSERT INTO foo (id, name) VALUES (67, 'hello, sqlite 067');
INSERT INTO foo (id, name) VALUES (68, 'hello, sqlite 068');
INSERT INTO foo (id, name) VALUES (69, 'hello, sqlite 069');
INSERT INTO foo (id, name) VALUES (70, 'hello, sqlite 070');
INSERT INTO foo (id, name) VALUES (71, 'hello, sqlite 071');
INSERT INTO foo (id, name) VALUES (72, 'hello, sqlite 072');
INSERT INTO foo (id, name) VALUES (73, 'hello, sqlite 073');
INSERT INTO foo (id, name) VALUES (74, 'hello, sqlite 074');
INSERT INTO foo (id, name) VALUES (75, 'hello, sqlite 075');
INSERT INTO foo (id, name) VALUES (76, 'hello, sqlite 076');
INSERT INTO foo (id, name) VALUES (77, 'hello, sqlite 077');
INSERT INTO foo (id, name) VALUES (78, 'hello, sqlite 078');
INSERT INTO foo (id, name) VALUES (79, 'hello, sqlite 079');
INSERT INTO foo (id, name) VALUES (80, 'hello, sqlite 080');
INSERT INTO foo (id, name) VALUES (81, 'hello, sqlite 081');
INSERT INTO foo (id, name) VALUES (82, 'hello, sqlite 082');
INSERT INTO foo (id, name) VALUES (83, 'hello, sqlite 083');
INSERT INTO foo (id, name) VALUES (84, 'hello, sqlite 084');
INSERT INTO foo (id, name) VALUES (85, 'hello, sqlite 085');
INSERT INTO foo (id, name) VALUES (86, 'hello, sqlite 086');
INSERT INTO foo (id, name) VALUES (87, 'hello, sqlite 087');
INSERT INTO foo (id, name) VALUES (88, 'hello, sqlite 088');
INSERT INTO foo (id, name) VALUES (89, 'hello, sqlite 089');
INSERT INTO foo (id, name) VALUES (90, 'hello, sqlite 090');
INSERT INTO foo (id, name) VALUES (91, 'hello, sqlite 091');
INSERT INTO foo (id, name) VALUES (92, 'hello, sqlite 092');
INSERT INTO foo (id, name) VALUES (93, 'hello, sqlite 093');
INSERT INTO foo (id, name) VALUES (94, 'hello, sqlite 094');
INSERT INTO foo (id, name) VALUES (95, 'hello, sqlite 095');
INSERT INTO foo (id, name) VALUES (96, 'hello, sqlite 096');
INSERT INTO foo (id, name) VALUES (97, 'hello, sqlite 097');
INSERT INTO foo (id, name) VALUES (98, 'hello, sqlite 098');
COMMIT;
INSERT INTO foo (id, name) VALUES (99, 'hello, sqlite 099');

SELECT name, id FROM foo WHERE id > 30 AND id < 40;

VACUUM INTO 'ATTACH_DB2';

DROP INDEX fooIdx;
VACUUM;

CRASH_AND_RECOVER;
ATTACH DATABASE 'ATTACH_DB1' AS other;
ATTACH DATABASE 'ATTACH_DB2' AS vacuumed;

SELECT name FROM foo WHERE id > 20 AND id <= 30
UNION
SELECT name FROM other.bar WHERE id > 10 AND id <= 20
UNION
SELECT name FROM vacuumed.foo WHERE id <= 10;

PRAGMA integrity_check;
