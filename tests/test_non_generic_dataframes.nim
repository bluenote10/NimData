import nimdata
import nimdata_utils
import strutils

UnitTestSuite("Non-generic DataFrames"):
  test "RangeDataFrame":
    check DF.fromRange(3).collect() == @[0, 1, 2]
    check DF.fromRange(3).map(x => x+1).collect() == @[1, 2, 3]
    check DF.fromRange(3).filter(x => x > 0).collect() == @[1, 2]
    check DF.fromRange(10).take(3).collect() == @[0, 1, 2]
    check DF.fromRange(5, 8).collect() == @[5, 6, 7]
    check DF.fromRange(-3, -1).collect() == @[-3, -2]
    check DF.fromRange(+3, -3).collect() == newSeq[int]()

  test "FileRowsDataFrame":
    check DF.fromFile("tests/data/mini.csv").count() == 5
    check DF.fromFile("tests/data/mini.csv", hasHeader=false).count() == 6
    const schema = [
      col(StrCol, "name"),
      col(IntCol, "age")
    ]
    let df = DF.fromFile("tests/data/mini.csv").map(schemaParser(schema, ';'))
    check df.count() == 5
    check df.filter(p => p.name.startsWith("B")).count() == 2
    check df.map(p => p.age).max() == 58
    # TODO: test with multiple separators
