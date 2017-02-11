
import future
import strutils
import math

import nimdata
import nimdata_utils


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


UnitTestSuite("Non-generic DataFrames"):
  test "RangeDataFrame":
    check DF.fromRange(3).collect() == @[0, 1, 2]
    check DF.fromRange(3).map(x => x+1).collect() == @[1, 2, 3]
    check DF.fromRange(3).filter(x => x > 0).collect() == @[1, 2]
    check DF.fromRange(10).take(3).collect() == @[0, 1, 2]
    check DF.fromRange(5, 8).collect() == @[5, 6, 7]
    check DF.fromRange(-3, -1).collect() == @[-3, -2]
    check DF.fromRange(+3, -3).collect() == newSeq[int]()


UnitTestSuite("Indexed Transformations"):
  test "take":
    let data = DF.fromSeq(@[1, 2, 3]).take(2)
    check data.count() == 2
    check data.collect() == @[1, 2]
    check data.cache().count() == 2
    check data.map(x => x).count() == 2
    check data.filter(x => true).count() == 2

    check DF.fromSeq(@[1, 2, 3]).take(0).count() == 0
    check DF.fromSeq(@[1, 2, 3]).take(1).count() == 1
    check DF.fromSeq(@[1, 2, 3]).take(2).count() == 2
    check DF.fromSeq(@[1, 2, 3]).take(3).count() == 3
    check DF.fromSeq(@[1, 2, 3]).take(4).count() == 3

    check DF.fromSeq(@[1, 2, 3]).take(2).collect() == @[1, 2]
    check DF.fromSeq(@[1, 2, 3]).take(2).take(2).collect() == @[1, 2]
    check DF.fromSeq(@[1, 2, 3]).take(2).take(2).take(2).collect() == @[1, 2]

  test "drop":
    let data = DF.fromSeq(@[1, 2, 3]).drop(2)
    check data.count() == 1
    check data.collect() == @[3]
    check data.cache().count() == 1
    check data.map(x => x).count() == 1
    check data.filter(x => true).count() == 1

    check DF.fromSeq(@[1, 2, 3]).drop(0).count() == 3
    check DF.fromSeq(@[1, 2, 3]).drop(1).count() == 2
    check DF.fromSeq(@[1, 2, 3]).drop(2).count() == 1
    check DF.fromSeq(@[1, 2, 3]).drop(3).count() == 0
    check DF.fromSeq(@[1, 2, 3]).drop(4).count() == 0

    check DF.fromSeq(@[1, 2, 3]).drop(1).collect() == @[2, 3]
    check DF.fromSeq(@[1, 2, 3]).drop(1).drop(1).collect() == @[3]
    check DF.fromSeq(@[1, 2, 3]).drop(1).drop(1).drop(1).collect() == newSeq[int]()
    check DF.fromSeq(@[1, 2, 3]).drop(1).drop(1).drop(1).drop(1).collect() == newSeq[int]()

  test "filterWithIndex":
    check DF.fromSeq(@[1, 2, 3]).filterWithIndex((i, x) => i == 1).collect() == @[2]

  test "mapWithIndex":
    check DF.fromSeq(@[1, 2, 3]).mapWithIndex((i, x) => i == 1).collect() == @[false, true, false]
    check DF.fromSeq(@[1, 2, 3]).mapWithIndex((i, x) => i*x).collect() == @[0, 2, 6]


UnitTestSuite("Reduce/Fold Actions"):
  test "reduce":
    check DF.fromSeq(@[1, 2, 3]).reduce((a, b) => a + b) == 6
    check DF.fromSeq(@[1, 2, 3]).reduce((a, b) => max(a, b)) == 3
    check DF.fromSeq(@[1, 2, 3]).reduce((a, b) => min(a, b)) == 1

  test "fold":
    check DF.fromSeq(@[1, 2, 3]).fold(0.0, (a, b) => a + b.float) == 6.0
    check DF.fromSeq(@[1, 2, 3]).fold(-Inf, (a, b) => max(a, b.float)) == 3.0
    check DF.fromSeq(@[1, 2, 3]).fold(+Inf, (a, b) => min(a, b.float)) == 1.0
    check DF.fromSeq(@[1, 2, 3]).fold("", (a, b) => a & $b) == "123"


UnitTestSuite("Numerical Actions"):
  test "sum":
    check DF.fromSeq(@[1, 2, 3]).sum() == 6
    check DF.fromSeq(@[1, 2, 3]).map(x => x).sum() == 6
    check DF.fromSeq(@[1, 2, 3]).filter(_ => false).sum() == 0

  test "mean":
    check DF.fromSeq(@[1, 2, 3]).mean() == 2
    check DF.fromSeq(@[1, 2, 3]).map(x => x).mean() == 2
    check DF.fromSeq(@[1, 2, 3]).filter(_ => false).mean().classify() == fcNaN

  test "min":
    check DF.fromSeq(@[1, 2, 3]).min() == 1
    check DF.fromSeq(@[1, 2, 3]).map(x => x).min() == 1
    check DF.fromSeq(@[1, 2, 3]).filter(_ => false).min() == high(int)
    check DF.fromSeq(@[-1, -2, -3]).min() == -3
    check DF.fromSeq(@[+1.0, +2.0, +3.0]).min() == 1.0
    check DF.fromSeq(@[-1.0, -2.0, -3.0]).min() == -3.0

  test "max":
    check DF.fromSeq(@[1, 2, 3]).max() == 3
    check DF.fromSeq(@[1, 2, 3]).map(x => x).max() == 3
    check DF.fromSeq(@[1, 2, 3]).filter(_ => false).max() == low(int)
    check DF.fromSeq(@[-1, -2, -3]).max() == -1
    check DF.fromSeq(@[+1.0, +2.0, +3.0]).max() == 3.0
    check DF.fromSeq(@[-1.0, -2.0, -3.0]).max() == -1.0


UnitTestSuite("Type specific"):
  test "DataFrame[string]":
    let data = DF.fromSeq(@["1", "2", "3"])
    check data.collect() == @["1", "2", "3"]
    check data.map(x => x & x).collect() == @["11", "22", "33"]



