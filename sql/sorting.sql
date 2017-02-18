CREATE TABLE a (a int);
INSERT INTO A VALUES (5);
INSERT INTO A VALUES (11);
INSERT INTO A VALUES (7);

SELECT * FROM run_select(''); -- todo, figure out why this first one fails

-- sort nodes aren't allowed to project, and a var doesn't make sense there
SELECT encode_protobuf($$
plan: {
  sort: {
    subplan: {
      sscan: { table: 1 }
      target: { var: { table: 1 column: "a" } }
    }
    col: { target: 1 ascending: false }
  }
  target: { var: { table: 1 column: "a" } }
}
rtable: { name: "a" }
$$) AS buf \gset
SELECT * FROM run_select(:'buf');

-- sort nodes aren't allowed to project
SELECT encode_protobuf($$
plan: {
  sort: {
    subplan: {
      sscan: { table: 1 }
      target: { var: { table: 1 column: "a" } }
    }
    col: { target: 1 ascending: false }
  }
  target: { leftRef: { target: 1 } }
}
rtable: { name: "a" }
$$) AS buf \gset
SELECT * FROM run_select(:'buf');

-- sort nodes also cannot select (use quals)
SELECT encode_protobuf($$
plan: {
  sort: {
    subplan: {
      sscan: { table: 1 }
      target: { var: { table: 1 column: "a" } }
    }
    col: { target: 1 ascending: false }
  }
  qual: { leftRef: { target: 1 } }
}
rtable: { name: "a" }
$$) AS buf \gset
SELECT * FROM run_select(:'buf');

-- SELECT a FROM a ORDER BY a DESC;
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

-- SELECT a FROM a ORDER BY a ASC;
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

-- SELECT a FROM a ORDER BY ;
-- sort nodes need at least one col!
SELECT encode_protobuf($$
plan: {
  sort: {
    subplan: {
      sscan: { table: 1 }
      target: { var: { table: 1 column: "a" } }
    }
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
INSERT INTO a VALUES (7, 9);
INSERT INTO a VALUES (1, 9);
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

-- SELECT a, b FROM a ORDER BY b ASC, a DESC;
SELECT encode_protobuf($$
plan: {
  sort: {
    subplan: {
      sscan: { table: 1 }
      target: { var: { table: 1 column: "a" } }
      target: { var: { table: 1 column: "b" } }
    }
    col: { target: 2 ascending: true }
    col: { target: 1 ascending: false }
  }
}
rtable: { name: "a" }
$$) AS buf \gset
SELECT * FROM run_select(:'buf');

-- SELECT a FROM a ORDER BY b ASC, a DESC;
-- TODO: this is expressable in SQL but not yet in pg_micromanage, it involves
-- resjunk target entries
SELECT encode_protobuf($$
plan: {
  sort: {
    subplan: {
      sscan: { table: 1 }
      target: { var: { table: 1 column: "a" } }
    }
    col: { target: 2 ascending: true }
    col: { target: 1 ascending: false }
  }
}
rtable: { name: "a" }
$$) AS buf \gset
SELECT * FROM run_select(:'buf');

-- SELECT a, b FROM a WHERE a.a = 1 ORDER BY b ASC, a DESC;
-- inner plans still support quals
SELECT encode_protobuf($$
plan: {
  sort: {
    subplan: {
      sscan: { table: 1 }
      target: { var: { table: 1 column: "a" } }
      target: { var: { table: 1 column: "b" } }
      qual: {
        op: {
	  name: "="
	  arg: { var: { table: 1 column: "a" } }
	  arg: { const: { uint: 1 } }
	}
      }
    }
    col: { target: 2 ascending: true }
    col: { target: 1 ascending: false }
  }
}
rtable: { name: "a" }
$$) AS buf \gset
SELECT * FROM run_select(:'buf');

-- stacking nodes should also work!
-- SELECT a.a FROM a INNER JOIN b ON (a.b = b.b) ORDER BY a.a ASC;
CREATE TABLE b (b int, c int);
INSERT INTO b VALUES (3, 5);
INSERT INTO b VALUES (7, 5);
INSERT INTO b VALUES (7, 6);
INSERT INTO b VALUES (1, 6);
INSERT INTO b VALUES (9, 3);

SELECT encode_protobuf($$
plan: {
  sort: {
    col: { target: 1 ascending: true }
    subplan: {
      join: {
        kind: NESTED
        type: INNER
        joinqual: {
          op: {
            name: "="
            arg: { leftRef: { target: 2 } }
            arg: { rightRef: { target: 1 } }
          }
        }
        left: {
          sscan: { table: 1 }
          target: { var: { table: 1 column: "a" } }
          target: { var: { table: 1 column: "b" } }
        }
        right: {
          sscan: { table: 2 }
          target: { var: { table: 2 column: "b" } }
        }
      }
      target: { leftRef: { target: 1 } }
      target: { leftRef: { target: 2 } }
    }
  }
}
rtable: { name: "a" }
rtable: { name: "b" }
$$) AS buf \gset
SELECT * FROM run_select(:'buf');

SELECT a.a, a.b FROM a INNER JOIN b ON (a.b = b.b) ORDER BY a.a ASC;

-- the other direction too, try to join the results of a sort

SELECT encode_protobuf($$
plan: {
  join: {
    kind: NESTED
    type: INNER
    joinqual: {
      op: {
        name: "="
        arg: { leftRef: { target: 2 } }
        arg: { rightRef: { target: 1 } }
      }
    }
    left: {
      sort: {
        col: { target: 1 ascending: true }
        subplan: {
          sscan: { table: 1 }
          target: { var: { table: 1 column: "a" } }
          target: { var: { table: 1 column: "b" } }
	}
      }
    }
    right: {
      sscan: { table: 2 }
      target: { var: { table: 2 column: "b" } }
    }
  }
  target: { leftRef: { target: 1 } }
  target: { leftRef: { target: 2 } }
}
rtable: { name: "a" }
rtable: { name: "b" }
$$) AS buf \gset
SELECT * FROM run_select(:'buf');

-- force it to do the same thing as us, so it returns tuples in the same order
SET enable_mergejoin TO false;
SET enable_hashjoin TO false;
SELECT sub.a, sub.b
FROM (SELECT a, b FROM a ORDER BY a.a) AS sub
INNER JOIN b ON (sub.b = b.b);

-- and just one more, the same test except the sortnode is in the right tree
SELECT encode_protobuf($$
plan: {
  join: {
    kind: NESTED
    type: INNER
    joinqual: {
      op: {
        name: "="
        arg: { leftRef: { target: 2 } }
        arg: { rightRef: { target: 1 } }
      }
    }
    left: {
      sscan: { table: 1 }
      target: { var: { table: 1 column: "a" } }
      target: { var: { table: 1 column: "b" } }
    }
    right: {
      # more efficient than it looks! It rewinds just like materialize would
      # I mean, still pointlesss but at least the rescan does not sort every time
      sort: {
        col: { target: 1 ascending: true }
        subplan: {
          sscan: { table: 2 }
          target: { var: { table: 2 column: "b" } }
	}
      }
    }
  }
  target: { leftRef: { target: 1 } }
  target: { leftRef: { target: 2 } }
}
rtable: { name: "a" }
rtable: { name: "b" }
$$) AS buf \gset
SELECT * FROM run_select(:'buf');

DROP TABLE a;
DROP TABLE b;
