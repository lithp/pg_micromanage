-- this first one always fails
SELECT * FROM run_select('');

CREATE TABLE a (a int);

-- refs should fail gracefully

SELECT encode_protobuf($$
plan: {
  sscan: { table: 1 }
  target: { rightRef: { target: 1 } }
}
rtable: { name: "a" }
$$) AS buf \gset
SELECT * FROM run_select(:'buf');


SELECT encode_protobuf($$
plan: {
  sscan: { table: 1 }
  target: { leftRef: { target: 1 } }
}
rtable: { name: "a" } 
$$) AS buf \gset
SELECT * FROM run_select(:'buf');

-- now for the actual tests

INSERT INTO a VALUES (1);
INSERT INTO a VALUES (10);

CREATE TABLE b (a int);
INSERT INTO b VALUES (1);

-- SELECT a.a FROM a INNER NESTEDLOOP JOIN b ON (a.a = b.a);
SELECT encode_protobuf($$
plan: {
  join: {
    kind: NESTED
    type: INNER
    joinqual: {
      op: {
        name: "="
        arg: { leftRef: { target: 1 } }
        arg: { rightRef: { target: 1 } }
      }
    }
    left: {
      sscan: { table: 1 }
      target: { var: { table: 1 column: "a" } }
    }
    right: {
      sscan: { table: 2 }
      target: { var: { table: 2 column: "a" } }
    }
  }
  target: { leftRef: { target: 1 } }
}
rtable: { name: "a" }
rtable: { name: "b" }
$$) AS buf \gset
SELECT * FROM run_select(:'buf');


-- SELECT a.a FROM a LEFT NESTEDLOOP JOIN b ON (a.a = b.a);
SELECT encode_protobuf($$
plan: {
  join: {
    kind: NESTED
    type: LEFT
    joinqual: {
      op: {
        name: "="
        arg: { leftRef: { target: 1 } }
        arg: { rightRef: { target: 1 } }
      }
    }
    left: {
      sscan: { table: 1 }
      target: {
        var: {
          table: 1
          column: "a"
        }
      }
    }
    right: {
      sscan: { table: 2 }
      target: {
        var: {
          table: 2
          column: "a"
        }
      }
    }
  }
  target: {
    leftRef: { target: 1 }
  }
}
rtable: { name: "a" }
rtable: { name: "b" }
$$) AS buf \gset
SELECT * FROM run_select(:'buf');


-- SELECT a.a FROM a RIGHT NESTEDLOOP JOIN b ON (a.a = b.a);
SELECT encode_protobuf($$
plan: {
  join: {
    kind: NESTED
    type: RIGHT
    joinqual: {
      op: {
        name: "="
        arg: { leftRef: { target: 1 } }
        arg: { rightRef: { target: 1 } }
      }
    }
    left: {
      sscan: { table: 1 }
      target: {
        var: {
          table: 1
          column: "a"
        }
      }
    }
    right: {
      sscan: { table: 2 }
      target: {
        var: {
          table: 2
          column: "a"
        }
      }
    }
  }
  target: {
    leftRef: { target: 1 }
  }
}
rtable: { name: "a" }
rtable: { name: "b" }
$$) AS buf \gset
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


-- SELECT a.b, a.a FROM a INNER NESTEDLOOP JOIN b ON (a.b = b.b);
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
      sscan: { table: 2 }
      target: { var: { table: 2 column: "b" } }
    }
  }
  target: { leftRef: { target: 2 } }
  target: { leftRef: { target: 1 } }
}
rtable: { name: "a" }
rtable: { name: "b" }
$$) AS buf \gset
SELECT * FROM run_select(:'buf');


-- SELECT a.a, XXX, a.b FROM a INNER NESTEDLOOP JOIN b ON (a.b = b.b);
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
      sscan: { table: 2 }
      target: { var: { table: 2 column: "b" } }
    }
  }
  target: { leftRef: { target: 1 } }
  target: { leftRef: { target: 3 } }
  target: { leftRef: { target: 2 } }
}
rtable: { name: "a" }
rtable: { name: "b" }
$$) AS buf \gset
SELECT * FROM run_select(:'buf');

DROP TABLE a;
DROP TABLE b;
