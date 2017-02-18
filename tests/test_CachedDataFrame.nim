import nimdata
import nimdata_utils

UnitTestSuite("CachedDataFrame"):
  test "Construction from seq":
    let data = DF.fromSeq(@[1, 2, 3])
    check data.count() == 3

  test "Iteration":
    let data = DF.fromSeq(@[1, 2, 3])
    let iter1 = data.iter()
    var count = 0
    for x in iter1():   # data.iter does not work; the ugly `(data.iter)()` does, but inf-loops
      count += 1
    check count == 3

  test "Collect":
    let data = DF.fromSeq(@[1, 2, 3])
    let result = data.collect()
    check result.len == 3
    check result == @[1, 2, 3]

  test "Cache":
    let data = DF.fromSeq(@[1, 2, 3]).cache()
    let result = data.collect()
    check result.len == 3
    check result == @[1, 2, 3]
