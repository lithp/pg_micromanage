CREATE EXTENSION micromanage;

-- no range table -> error!
SELECT encode_protobuf($$
plan: {
  sscan: { table: 1 }
  target: { var: { table: 1 column: "a" } }
}
$$) AS buf \gset
SELECT * FROM run_select(:'buf');


-- SELECT a FROM a;
SELECT encode_protobuf($$
plan: {
  sscan: { table: 1 }
  target: { var: { table: 1 column: "a" } }
}
rtable: { name: "a" }
$$) AS buf \gset
SELECT * FROM run_select(:'buf');

CREATE TABLE a (a int);
INSERT INTO a VALUES (1);
INSERT INTO a VALUES (10);

SELECT * FROM run_select(:'buf');


-- SELECT d FROM a;
SELECT encode_protobuf($$
plan: {
  sscan: { table: 1 }
  target: { var: { table: 1 column: "d" } }
}
rtable: { name: "a" }
$$) AS buf \gset
SELECT * FROM run_select(:'buf');


-- this is not expressable in sql, it should definitely fail!
SELECT encode_protobuf($$
plan: {
  sscan: { table: 1 }
  target: { var: { table: 2 column: "d" } }
}
rtable: { name: "a" }
$$) AS buf \gset
SELECT * FROM run_select(:'buf');


-- SELECT a, 10 FROM a;
SELECT encode_protobuf($$
plan: {
  sscan: { table: 1 }
  target: { var: { table: 1 column: "a" } }
  target: { const: { uint: 10 } }
}
rtable: { name: "a" }
$$) AS buf \gset
SELECT * FROM run_select(:'buf');


-- SELECT a + 10 FROM a;
SELECT encode_protobuf($$
plan: {
  sscan: { table: 1 }
  target: {
    op: {
      name: "+"
      arg: { var: { table: 1 column: "a" } }
      arg: { const: { uint: 10 } }
    }
  }
}
rtable: { name: "a" }
$$) AS buf \gset
SELECT * FROM run_select(:'buf');


-- SELECT a, a > 5 FROM a;
SELECT encode_protobuf($$
plan: {
  sscan: { table: 1 }
  target: { var: { table: 1 column: "a" } }
  target: {
    op: {
      name: ">"
      arg: { var: { table: 1 column: "a" } }
      arg: { const: { uint: 5 }
      }
    }
  }
}
rtable: { name: "a" }
$$) AS buf \gset
SELECT * FROM run_select(:'buf');


-- SELECT a FROM a WHERE a = 10;
SELECT encode_protobuf($$
plan: {
  sscan: { table: 1 }
  target: { var: { table: 1 column: "a" } }
  qual: {
    op: {
      name: "="
      arg: { var: { table: 1 column: "a" } }
      arg: { const: { uint: 10 } }
    }
  }
}
rtable: { name: "a" }
$$) AS buf \gset
SELECT * FROM run_select(:'buf');


-- SELECT a FROM a WHERE a + 10;
SELECT encode_protobuf($$
plan: {
  sscan: { table: 1 }
  target: { var: { table: 1 column: "a" } }
  qual: {
    op: {
      name: "+"
      arg: { var: { table: 1 column: "a" } }
      arg: { const: { uint: 10; } }
    }
  }
}
rtable: { name: "a" } 
$$) AS buf \gset
SELECT * FROM run_select(:'buf');


-- SELECT a FROM a WHERE true;
SELECT encode_protobuf($$
plan: {
  sscan: { table: 1 }
  target: { var: { table: 1 column: "a" } }
  qual: { const: { bool: true } }
}
rtable: { name: "a" }
$$) AS buf \gset
SELECT * FROM run_select(:'buf');


-- SELECT a FROM a WHERE false;
SELECT encode_protobuf($$
plan: {
  sscan: { table: 1 }
  target: { var: { table: 1 column: "a" } }
  qual: { const: { bool: false } }
}
rtable: { name: "a" }
$$) AS buf \gset
SELECT * FROM run_select(:'buf');


-- SELECT a + true FROM a;
SELECT encode_protobuf($$
plan: {
  sscan: { table: 1 }
  target: {
    op: {
      name: "+"
      arg: { var: { table: 1 column: "a" } }
      arg: { const: { bool: true } }
    }
  }
}
rtable: { name: "a" }
$$) AS buf \gset
SELECT * FROM run_select(:'buf');


-- SELECT a = true FROM a;
SELECT encode_protobuf($$
plan: {
  sscan: { table: 1 }
  target: {
    op: {
      name: "="
      arg: { var: { table: 1 column: "a" } }
      arg: { const: { bool: true } }
    }
  }
}
rtable: { name: "a" }
$$) AS buf \gset
SELECT * FROM run_select(:'buf');

CREATE TABLE b (a int);

-- adding unused range tables is okay
SELECT encode_protobuf($$
plan: {
  sscan: { table: 1 }
  target: { var: { table: 1 column: "a" } }
}
rtable: { name: "a" }
rtable: { name: "b" }
$$) AS buf \gset
SELECT * FROM run_select(:'buf');

-- SELECT a.a, b.a + 10 FROM a
SELECT encode_protobuf($$
plan: {
  sscan: { table: 1 }
  target: { var: { table: 1 column: "a" } }
  target: {
    op: {
      name: "+"
      arg: { var: { table: 2 column: "a" } }
      arg: { const: { uint: 10 } }
    }
  }
  qual: {
    op: {
      name: "="
      arg: { var: { table: 1 column: "a" } }
      arg: { const: { uint: 10 } }
    }
  }
}
rtable: { name: "a" }
rtable: { name: "b" }
$$) AS buf \gset
SELECT * FROM run_select(:'buf');

-- SELECT a.a FROM a WHERE b.a = 10
SELECT encode_protobuf($$
plan: {
  sscan: { table: 1 }
  target: { var: { table: 1 column: "a" } }
  qual: {
    op: {
      name: "="
      arg: { var: { table: 2 column: "a" } }
      arg: { const: { uint: 10 } }
    }
  }
}
rtable: { name: "a" }
rtable: { name: "b" }
$$) AS buf \gset
SELECT * FROM run_select(:'buf');

DROP TABLE a;
DROP TABLE b;
