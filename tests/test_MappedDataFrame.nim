import nimdata
import nimdata_utils
import strutils

UnitTestSuite("MappedDataFrame"):
  test "Construction":
    let data = DF.fromSeq(@[1, 2, 3])
    let mapped1 = data.map(x => x*2)
    let mapped2 = DF.fromSeq(@[1, 2, 3]).map(x => x*2)
    let mapped3 = data.map(x => x*3).map(x => x*4)
    check mapped1.count() == 3
    check mapped2.count() == 3
    check mapped3.count() == 3

  test "Iteration":
    let data = DF.fromSeq(@[1, 2, 3]).map(x => x*2)
    let it = data.iter()
    var count = 0
    for x in it():   # data.iter does not work, it is the ugly `(data.iter)()` compiles but inf-loops
      count += 1
    check count == 3

  test "Collect":
    let data = DF.fromSeq(@[1, 2, 3]).map(x => x*2)
    let result = data.collect()
    check result.len == 3
    check result == @[2, 4, 6]

  test "Cache":
    let data = DF.fromSeq(@[1, 2, 3]).map(x => x*2).cache()
    let result = data.collect()
    check result.len == 3
    check result == @[2, 4, 6]

  test "Composition":
    let data = DF.fromSeq(@[1, 2, 3])
    let mapped = data.map(x => x*2)
    check mapped.collect() == @[2, 4, 6]
    check data.map(x => x*2).collect() == @[2, 4, 6]
    check data.map(x => x*2).map(x => x*2).collect() == @[4, 8, 12]
    check data.filter(x => x mod 2 == 1).map(x => x * 100).collect() == @[100, 300]

  test "Type Conversion":
    proc convert(i: int): string = $i
    check DF.fromSeq(@[1, 2, 3]).map(convert).collect() == @["1", "2", "3"]
    check DF.fromSeq(@[1, 2, 3]).map(x => $x).collect() == @["1", "2", "3"]
    check DF.fromSeq(@[1, 2, 3]).map(x => x == 2).collect() == @[false, true, false]
    check DF.fromSeq(@[1, 2, 3]).map(x => x.float).collect() == @[1.0, 2.0, 3.0]

    check DF.fromSeq(@["1", "2", "3"]).map(x => x.parseInt).collect() == @[1, 2, 3]
    check DF.fromSeq(@["1", "2", "3"]).map(x => x.parseFloat).collect() == @[1.0, 2.0, 3.0]

