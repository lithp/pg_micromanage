plan: {
  sscan: {
    table: 1
  }
  target: {
    var: {
      table: 1
      column: "a"
    }
  }
  qual: {
    op: {
      name: "="
      arg: {
        var: {
          table: 1
          column: "a"
        }
      }
      arg: {
        const: {
          uint: 10;
        }
      }
    }
  }
}
rtable: {
  name: "a"
}
