# pg_micromanage

Usage
-----

```
CREATE EXTENSION micromanage
SELECT * FROM run_select('CgIIARIDCgFh');
```

Where the parameter passed in to `run_select` is a protocol buffer, as described in
`queries.proto`. You'll find many examples in the `example-messages` directory, they
look like this:

```
plan: {
  sscan: { table: 1 }
  target: {
    var: {
      table: 1
      column: "a"
    }
  }
}
rtable: { name: "a" }
```

To turn it into something run_select will accept, run a command like this:

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
