import nimdata
import nimdata_utils

UnitTestSuite("UniqueDataFrame"):
  test "unique":
    check DF.fromSeq(@[1, 1, 2]).unique().collect() == @[1, 2]
    check DF.fromSeq(@[1, 2, 1]).unique().collect() == @[1, 2]
    check DF.fromSeq(@[2, 1, 1]).unique().collect() == @[2, 1]
    check DF.fromSeq(@["A", "B", "B", "A", "C", "A"]).unique().collect() == @["A", "B", "C"]
    check DF.fromRange(0, 10).unique().count() == 10
    let tupleDataDuplicates = @[
      (id: 1, name: "A"),
      (id: 1, name: "A"),
      (id: 2, name: "X"),
      (id: 1, name: "A"),
      (id: 2, name: "X"),
      (id: 2, name: "A"),
      (id: 1, name: "X"),
    ]
    let tupleDataExpected = @[
      (id: 1, name: "A"),
      (id: 2, name: "X"),
      (id: 2, name: "A"),
      (id: 1, name: "X"),
    ]
    check DF.fromSeq(tupleDataDuplicates).unique().collect() == tupleDataExpected
