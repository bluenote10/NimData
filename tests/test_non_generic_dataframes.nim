import nimdata
import nimdata/utils
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
    check DF.fromFile("tests/data/mini.csv", FileType.RawText).count() == 5
    check DF.fromFile("tests/data/mini.csv", FileType.RawText, hasHeader=false).count() == 6
    const schema = [
      strCol("name"),
      intCol("age")
    ]
    let df = DF.fromFile("tests/data/mini.csv", FileType.RawText).map(schemaParser(schema, ';'))
    check df.count() == 5
    check df.filter(p => p.name.startsWith("B")).count() == 2
    check df.map(p => p.age).max() == 58
    # TODO: test with multiple separators

  test "FileRowsDataFrameGZip":
    check DF.fromFile("tests/data/mini.csv.gz", FileType.GZip).count() == 5
    check DF.fromFile("tests/data/mini.csv.gz", FileType.GZip, hasHeader=false).count() == 6
    const schema = [
      strCol("name"),
      intCol("age")
    ]
    let df = DF.fromFile("tests/data/mini.csv.gz", FileType.GZip).map(schemaParser(schema, ';'))
    check df.count() == 5
    check df.filter(p => p.name.startsWith("B")).count() == 2
    check df.map(p => p.age).max() == 58
    # TODO: test with multiple separators

  test "Smart file type detection":
    for file in ["tests/data/mini.csv", "tests/data/mini.csv.gz"]:
      check DF.fromFile(file).count() == 5
      check DF.fromFile(file, hasHeader=false).count() == 6
      const schema = [
        strCol("name"),
        intCol("age")
      ]
      let df = DF.fromFile(file).map(schemaParser(schema, ';'))
      check df.count() == 5
      check df.filter(p => p.name.startsWith("B")).count() == 2
      check df.map(p => p.age).max() == 58
