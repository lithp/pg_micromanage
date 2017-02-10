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
      name: "+"
      arg: {
        var: {
          table: 1
          column: "b"
        }
      }
      arg: {
        const: {
          int: 10
        }
      }
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
          int: 10
        }
      }
    }
  }
}
rtable: {
  name: "a"
}
rtable: {
  name: "b"
}
