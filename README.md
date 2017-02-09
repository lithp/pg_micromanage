# pg_micromanage

Usage:

```
CREATE EXTENSION micromanage
SELECT * FROM run_select('CgIIARIDCgFh');
```

Where the parameter passed in to `run_select` is a protocol buffer, as described in
`queries.proto`.

To create one of these strings, create a file with contents such as this:

```
sscan: {
 table: 1
}
rtable: {
 name: "b"
}
```

Then run `cat example.msg | protoc queries.proto --encode=SelectQuery | base64`.
