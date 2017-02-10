sscan: {
  table: 1
  target: {
    var: {
      table: 1
      column: "a"
    }
  }
  target: {
    op: {
      name: ">"
      arg: {
        var: {
          table: 1
          column: "a"
        }
      }
      arg: {
        const: {
          uint: 5
        }
      }
    }
  }
}
rtable: {
  name: "a"
}
