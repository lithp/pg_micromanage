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
