import nimdata
import nimdata_utils

UnitTestSuite("FilteredDataFrame"):
  test "Construction":
    let data = DF.fromSeq(@[1, 2, 3])
    let filtered1 = data.filter(x => x > 1)
    let filtered2 = DF.fromSeq(@[1, 2, 3]).filter(x => x > 1)
    let filtered3 = data.filter(x => x > 1).filter(x => x > 2)
    check filtered1.count() == 2
    check filtered2.count() == 2
    check filtered3.count() == 1

  test "Iteration":
    let data = DF.fromSeq(@[1, 2, 3]).filter(x => x > 1)
    let it = data.iter()
    var count = 0
    for x in it():
      count += 1
    check count == 2

  test "Collect":
    let data = DF.fromSeq(@[1, 2, 3]).filter(x => x > 1)
    let result = data.collect()
    check result.len == 2
    check result == @[2, 3]

  test "Cache":
    let data = DF.fromSeq(@[1, 2, 3]).filter(x => x > 1).cache()
    let result = data.collect()
    check result.len == 2
    check result == @[2, 3]

  test "Composition":
    let data = DF.fromSeq(@[1, 2, 3])
    check data.map(x => x * 100).filter(x => x mod 2 == 1).collect() == newSeq[int]()
    check data.filter(x => x mod 2 == 1).map(x => x * 100).collect() == @[100, 300]
