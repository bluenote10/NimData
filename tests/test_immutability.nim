import nimdata
import nimdata_utils

UnitTestSuite("Immutability"):
  test "Scalars":
    let dataInts = @[1, 2, 3]
    discard DF.fromSeq(dataInts).map(x => x + 1).count()
    check dataInts == @[1, 2, 3]
    var dataCollect = DF.fromSeq(dataInts).collect()
    dataCollect[0] = 0
    check dataInts == @[1, 2, 3]
    check dataCollect == @[0, 2, 3]

  test "Ref types 1":
    let dataStrings = @["1", "2", "3"]
    discard DF.fromSeq(dataStrings).map(x => x & $1).count()
    check dataStrings == @["1", "2", "3"]
    var dataCollect = DF.fromSeq(dataStrings).collect()
    dataCollect[0] &= $1
    check dataStrings == @["1", "2", "3"]
    check dataCollect == @["11", "2", "3"]

  test "Ref types 2":
    var dataOrig = @["1", "2", "3"]
    var dataCopy = DF.fromSeq(dataOrig).collect()
    dataOrig[2] &= "!"
    check dataOrig == @["1", "2", "3!"]
    check dataCopy == @["1", "2", "3"]

  test "Tuples":
    let data = @[
      (colA: 1, colB: "1"),
      (colA: 2, colB: "2"),
      (colA: 3, colB: "3"),
    ]
    check:
      notCompiles: DF.fromSeq(data).forEach(proc (x: tuple): void = x.colA = "0")
      notCompiles: DF.fromSeq(data).forEach(proc (x: tuple): void = x.colA &= "x")
      notCompiles: DF.fromSeq(data).forEach(proc (x: var tuple): void = x.colA &= "x")
