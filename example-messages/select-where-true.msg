plan: {
  sscan: {
    table: 1
    target: {
      var: {
        table: 1
        column: "a"
      }
    }
    qual: {
      const: {
        bool: true
      }
    }
  }
}
rtable: {
  name: "a"
}
