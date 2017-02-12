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
