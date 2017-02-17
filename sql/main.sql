CREATE EXTENSION micromanage;

-- SELECT a FROM a;
\set buf `cat example-messages/select-a-from-a.msg | protoc queries.proto --encode=SelectQuery | base64 -w0`
SELECT * FROM run_select(:'buf');

CREATE TABLE a (a int);
INSERT INTO a VALUES (1);
INSERT INTO a VALUES (10);

SELECT * FROM run_select(:'buf');

-- SELECT d FROM a;
\set buf `cat example-messages/select-d-from-a.msg | protoc queries.proto --encode=SelectQuery | base64 -w0`
SELECT * FROM run_select(:'buf');

-- this is not expressable in sql, it should definitely fail!
\set buf `cat example-messages/select-d-from-bad-index.msg | protoc queries.proto --encode=SelectQuery | base64 -w0`
SELECT * FROM run_select(:'buf');

-- SELECT a, 10 FROM a;
\set buf `cat example-messages/select-const-from-a.msg | protoc queries.proto --encode=SelectQuery | base64 -w0`
SELECT * FROM run_select(:'buf');

-- SELECT a + 10 FROM a;
\set buf `cat example-messages/select-a-plus-10.msg | protoc queries.proto --encode=SelectQuery | base64 -w0`
SELECT * FROM run_select(:'buf');

-- SELECT a, a > 5 FROM a;
\set buf `cat example-messages/select-a-gt-5.msg | protoc queries.proto --encode=SelectQuery | base64 -w0`
SELECT * FROM run_select(:'buf');

-- SELECT a FROM a WHERE a = 10;
\set buf `cat example-messages/select-where-a-eq.msg | protoc queries.proto --encode=SelectQuery | base64 -w0`
SELECT * FROM run_select(:'buf');

-- SELECT a FROM a WHERE a + 10;
\set buf `cat example-messages/select-with-non-bool-qual.msg | protoc queries.proto --encode=SelectQuery | base64 -w0`
SELECT * FROM run_select(:'buf');

-- SELECT a FROM a WHERE true;
\set buf `cat example-messages/select-where-true.msg | protoc queries.proto --encode=SelectQuery | base64 -w0`
SELECT * FROM run_select(:'buf');

-- SELECT a FROM a WHERE false;
\set buf `cat example-messages/select-where-false.msg | protoc queries.proto --encode=SelectQuery | base64 -w0`
SELECT * FROM run_select(:'buf');

-- SELECT a + true FROM a;
\set buf `cat example-messages/select-a-plus-true.msg | protoc queries.proto --encode=SelectQuery | base64 -w0`
SELECT * FROM run_select(:'buf');

-- SELECT a = true FROM a;
\set buf `cat example-messages/select-a-eq-true.msg | protoc queries.proto --encode=SelectQuery | base64 -w0`
SELECT * FROM run_select(:'buf');

-- not expressible, should gracefully error
\set buf `cat example-messages/select-inner-from-a.msg | protoc queries.proto --encode=SelectQuery | base64 -w0`
SELECT * FROM run_select(:'buf');

-- not expressible, should gracefully error
\set buf `cat example-messages/select-outer-from-a.msg | protoc queries.proto --encode=SelectQuery | base64 -w0`
SELECT * FROM run_select(:'buf');

CREATE TABLE b (a int);
INSERT INTO b VALUES (1);
-- SELECT a.a FROM a INNER NESTEDLOOP JOIN b ON (a.a = b.a);
\set buf `cat example-messages/simple-inner-nestedloop.msg | protoc queries.proto --encode=SelectQuery | base64 -w0`
SELECT * FROM run_select(:'buf');

-- SELECT a.a FROM a LEFT NESTEDLOOP JOIN b ON (a.a = b.a);
\set buf `cat example-messages/simple-left-nestedloop.msg | protoc queries.proto --encode=SelectQuery | base64 -w0`
SELECT * FROM run_select(:'buf');

-- SELECT a.a FROM a RIGHT NESTEDLOOP JOIN b ON (a.a = b.a);
\set buf `cat example-messages/simple-right-nestedloop.msg | protoc queries.proto --encode=SelectQuery | base64 -w0`
SELECT * FROM run_select(:'buf');

-- SELECT a.a FROM a ORDER BY a DESC;
INSERT INTO A VALUES (5);
INSERT INTO A VALUES (11);
INSERT INTO A VALUES (7);
\set buf `cat example-messages/select-a-desc.msg | protoc queries.proto --encode=SelectQuery | base64 -w0`
SELECT * FROM run_select(:'buf');

-- SELECT a.a FROM a ORDER BY a ASC;
\set buf `cat example-messages/select-a-asc.msg | protoc queries.proto --encode=SelectQuery | base64 -w0`
SELECT * FROM run_select(:'buf');

TRUNCATE a;
ALTER TABLE a ADD COLUMN b int;
INSERT INTO a VALUES (1, 7);
INSERT INTO a VALUES (1, 10);
INSERT INTO a VALUES (5, 2);
INSERT INTO a VALUES (5, 9);
INSERT INTO a VALUES (3, 1);

-- SELECT a.a, a.b FROM a ORDER BY a ASC, b DESC;
\set buf `cat example-messages/select-a-asc-b-desc.msg | protoc queries.proto --encode=SelectQuery | base64 -w0`
SELECT * FROM run_select(:'buf');

DROP TABLE a;
DROP TABLE b;
CREATE TABLE a (a int, b int);
CREATE TABLE b (b int, c int);
INSERT INTO a VALUES (1, 1);
INSERT INTO a VALUES (1, 2);
INSERT INTO b VALUES (2, 1);
INSERT INTO b VALUES (2, 3);
INSERT INTO b VALUES (3, 1);

SELECT a.a, a.b FROM a JOIN b ON (a.b = b.b);

-- SELECT a.a, a.b FROM a INNER NESTEDLOOP JOIN b ON (a.b = b.b);
\set buf `cat example-messages/inner-nestedloop-multiple-col.msg | protoc queries.proto --encode=SelectQuery | base64 -w0`
SELECT * FROM run_select(:'buf');

-- SELECT a.b, a.a FROM a INNER NESTEDLOOP JOIN b ON (a.b = b.b);
\set buf `cat example-messages/inner-nestedloop-multiple-col-reversed.msg | protoc queries.proto --encode=SelectQuery | base64 -w0`
SELECT * FROM run_select(:'buf');

-- SELECT a.a, XXX, a.b FROM a INNER NESTEDLOOP JOIN b ON (a.b = b.b);
\set buf `cat example-messages/inner-nestedloop-multiple-col-noexist.msg | protoc queries.proto --encode=SelectQuery | base64 -w0`
SELECT * FROM run_select(:'buf');
