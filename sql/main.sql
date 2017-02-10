CREATE EXTENSION micromanage;

\set buf `cat example-messages/select-a-from-a.msg | protoc queries.proto --encode=SelectQuery | base64 -w0`
SELECT * FROM run_select(:'buf');

CREATE TABLE a (a int);
INSERT INTO a VALUES (1);

SELECT * FROM run_select(:'buf');

\set buf `cat example-messages/select-d-from-a.msg | protoc queries.proto --encode=SelectQuery | base64 -w0`
SELECT * FROM run_select(:'buf');

\set buf `cat example-messages/select-d-from-bad-index.msg | protoc queries.proto --encode=SelectQuery | base64 -w0`
SELECT * FROM run_select(:'buf');

\set buf `cat example-messages/select-const-from-a.msg | protoc queries.proto --encode=SelectQuery | base64 -w0`
SELECT * FROM run_select(:'buf');
