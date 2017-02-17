# SELECT a, b FROM a ORDER BY a ASC, b DESC;
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
