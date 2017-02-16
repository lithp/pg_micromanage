# SELECT a FROM a ORDER BY a ASC;
plan: {
  sort: {
    subplan: {
      sscan: { table: 1 }
      target: {
        var: {
          table: 1
          column: "a"
        }
      }
    }
    col: {
      target: 1
      ascending: true
    }
  }
  target: {
    leftRef: {
      target: 1
    }
  }
}
rtable: {
  name: "a"
}
