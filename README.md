# pg_micromanage

Usage
-----

```psql
# Some setup
psql=# CREATE EXTENSION micromanage;
CREATE EXTENSION
psql=# CREATE TABLE a (a int);
CREATE TABLE
psql=# INSERT INTO a VALUES (1);
INSERT 0 1

# Create the protobuf
psql=# SELECT encode_protobuf($$
psql$# plan: {
psql$#   sscan: { table: 1 }
psql$#   target: { var: { table: 1 column: "a" } }
psql$# }
psql$# rtable: { name: "a" }
psql$# $$) AS buf \gset

# Run the query!
psql=# SELECT * FROM run_select(:'buf');
 a 
---
 1
(1 row)

# You can also generate protobufs yourself and use them directly:
psql=# SELECT * FROM run_select('Cg0KBwoFCAESAWEaAggBEgMKAWE=');
 a 
---
 1
(1 row)
```

Queries are described in `queries.proto`. You'll find many examples in the tests, which
are in the `sql` directory. You can see the results of running the commands found in `sql` in the `expected` directory.

If you want to make your own protobufs do something like this:

```
cat example.msg | protoc queries.proto --encode=SelectQuery | base64
```

Implemented
-----------

- sequence scans
- extremely simple expressions
- where clauses
- nested loop joins
- sorting (order by)

Requirements
-------------

- `protobuf-c` and `protoc`. I think they're the `protobuf-c-dev` and `protobuf-compiler`
  packages on Ubuntu.
- Postgres 9.6

Building
--------

```
export PG_CONFIG=/home/brian/Work/pg-961/bin/pg_config # obviously not this exact string
make
```

Running the tests
-----------------

This requires that you already have a postgres database running locally.

```
make install
make installcheck
```
