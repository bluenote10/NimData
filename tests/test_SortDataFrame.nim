import nimdata
import nimdata/utils

UnitTestSuite("SortDataFrame"):
  test "sort":
    check DF.fromSeq(@[2, 3, 1]).sort().collect() == @[1, 2, 3]
    check DF.fromSeq(@[2, 3, 1]).sort(SortOrder.Descending).collect() == @[3, 2, 1]
    check DF.fromSeq(@[2, 3, 1]).sort(x => -x).collect() == @[3, 2, 1]
    check DF.fromSeq(@[2, 3, 1]).sort(x => -x, SortOrder.Descending).collect() == @[1, 2, 3]
    let data = @[
      (colA: "X", colB: 5, colC: 4.8),
      (colA: "Z", colB: 3, colC: 3.1),
      (colA: "Y", colB: 4, colC: 2.3),
      (colA: "B", colB: 1, colC: 5.2),
      (colA: "A", colB: 2, colC: 1.5),
    ]
    check DF.fromSeq(data).sort(x => x.colA).map(x => x.colB).collect() == @[2, 1, 5, 4, 3]
    check DF.fromSeq(data).sort(x => x.colB).map(x => x.colB).collect() == @[1, 2, 3, 4, 5]
    check DF.fromSeq(data).sort(x => x.colC).map(x => x.colB).collect() == @[2, 4, 3, 5, 1]

