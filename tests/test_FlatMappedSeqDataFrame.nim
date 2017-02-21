import nimdata
import nimdata/utils
import strutils

UnitTestSuite("FlatMappedSeqDataFrame"):
  test "flatMap":
    let data = DF.fromSeq(@[1, 3, 5])

    check data.flatMap(x => @[x, x+1]).collect() == @[1, 2, 3, 4, 5, 6]
    check data.flatMap(x => newSeq[int]()).collect() == newSeq[int]()
    check data.flatMap(x => @[$x]).collect() == @["1", "3", "5"]
