import nimdata
import nimdata_utils

UnitTestSuite("Specific types"):
  test "DataFrame[string]":
    let data = DF.fromSeq(@["1", "2", "3"])
    check data.collect() == @["1", "2", "3"]
    check data.map(x => x & x).collect() == @["11", "22", "33"]
