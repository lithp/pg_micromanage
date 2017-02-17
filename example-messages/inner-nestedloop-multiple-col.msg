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
