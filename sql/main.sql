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
