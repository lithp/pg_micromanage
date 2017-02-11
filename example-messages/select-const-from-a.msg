plan: {
  sscan: {
    table: 1
    target: {
      var: {
        table: 1
        column: "a"
      }
    }
    target: {
      const: {
        uint: 10
      }
    }
  }
}
rtable: {
  name: "a"
}
