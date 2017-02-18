CREATE TABLE a (a int);
INSERT INTO A VALUES (5);
INSERT INTO A VALUES (11);
INSERT INTO A VALUES (7);

SELECT * FROM run_select(''); -- todo, figure out why this first one fails

-- SELECT d FROM a ORDER BY a DESC;
SELECT encode_protobuf($$
plan: {
  sort: {
    subplan: {
      sscan: { table: 1 }
      target: { var: { table: 1 column: "a" } }
    }
    col: { target: 1 ascending: false }
  }
}
rtable: { name: "a" }
$$) AS buf \gset
SELECT * FROM run_select(:'buf');

-- SELECT d FROM a ORDER BY a ASC;
SELECT encode_protobuf($$
plan: {
  sort: {
    subplan: {
      sscan: { table: 1 }
      target: { var: { table: 1 column: "a" } }
    }
    col: { target: 1 ascending: true }
  }
}
rtable: { name: "a" }
$$) AS buf \gset
SELECT * FROM run_select(:'buf');

TRUNCATE a;
ALTER TABLE a ADD COLUMN b int;
INSERT INTO a VALUES (1, 7);
INSERT INTO a VALUES (1, 10);
INSERT INTO a VALUES (5, 2);
INSERT INTO a VALUES (5, 9);
INSERT INTO a VALUES (3, 1);

-- SELECT a, b FROM a ORDER BY a ASC, b DESC;
SELECT encode_protobuf($$
plan: {
  sort: {
    subplan: {
      sscan: { table: 1 }
      target: { var: { table: 1 column: "a" } }
      target: { var: { table: 1 column: "b" } }
    }
    col: { target: 1 ascending: true }
    col: { target: 2 ascending: false }
  }
}
rtable: { name: "a" }
$$) AS buf \gset
SELECT * FROM run_select(:'buf');

DROP TABLE a;
