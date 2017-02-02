
import future

import ../src/nimdata
import ../src/nimdata_utils


UnitTestSuite("Schema Parser"):
  test "Parsing":
    const schema = [
      col(IntCol, "columnA"),
      col(IntCol, "columnB")
    ]
    let parser = schemaParser(schema)
    let result = parser("1;2")
    check result == (columnA: 1, columnB: 2)


UnitTestSuite("CachedDataFrame"):
  test "Construction from seq":
    let data = newPersistedDataFrame[int](@[1, 2, 3])

  test "Iteration":
    let data = newPersistedDataFrame[int](@[1, 2, 3])
    let iter1 = data.iter()
    var count = 0
    for x in iter1():   # data.iter does not work, it is the ugly `(data.iter)()` compiles but inf-loops
      count += 1
    check count == 3

  test "Collect":
    let data = newPersistedDataFrame[int](@[1, 2, 3])
    let result = data.collect()
    check result.len == 3
    check result == @[1, 2, 3]


UnitTestSuite("MappedDataFrame"):
  test "Contstruction":
    let data = newPersistedDataFrame[int](@[1, 2, 3])
    let mapped1 = data.map(x => x*2)
    let mapped2 = newPersistedDataFrame[int](@[1, 2, 3]).map(x => x*2)
    let mapped3 = data.map(x => x*3).map(x => x*4)

  test "Iteration":
    let data = newPersistedDataFrame[int](@[1, 2, 3]).map(x => x*2)
    let it = data.iter()
    var count = 0
    for x in it():   # data.iter does not work, it is the ugly `(data.iter)()` compiles but inf-loops
      count += 1
    check count == 3

  test "Collect":
    let data = newPersistedDataFrame[int](@[1, 2, 3]).map(x => x*2)
    let result = data.collect()
    check result.len == 3
    check result == @[2, 4, 6]

  test "Composition":
    let data = newPersistedDataFrame[int](@[1, 2, 3])
    let mapped = data.map(x => x*2)
    check mapped.collect() == @[2, 4, 6]
    check data.map(x => x*2).collect() == @[2, 4, 6]
    check data.map(x => x*2).map(x => x*2).collect() == @[4, 8, 12]
    check data.filter(x => x mod 2 == 1).map(x => x * 100).collect() == @[100, 300]


