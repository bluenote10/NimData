import nimdata
import nimdata/utils
import streams
import strutils

UnitTestSuite("Show"):

  let stdoutDummy = newFileStream("tests/show.log", fmWrite)

  test "fixedTruncate":
    check fixedTruncateR("test", 0) == "…"
    check fixedTruncateR("test", 1) == "…"
    check fixedTruncateR("test", 2) == "t…"
    check fixedTruncateR("test", 3) == "te…"
    check fixedTruncateR("test", 4) == "test"
    check fixedTruncateR("test", 5) == "test"
    check fixedTruncateL("test", 0) == "…"
    check fixedTruncateL("test", 1) == "…"
    check fixedTruncateL("test", 2) == "…t"
    check fixedTruncateL("test", 3) == "…st"
    check fixedTruncateL("test", 4) == "test"
    check fixedTruncateL("test", 5) == "test"

  test "Basic types":
    var s = newStringStream()
    DF.fromRange(3).show(stdoutDummy)
    DF.fromRange(3).show(s)
    s.setPosition(0)
    check: s.readAll() == "0\n1\n2\n"

  test "Tuples":
    let data1 = @[
      (name: "Bob", age: 99, testWithLongColumn: 1.112341975, anotherCol: "with very long strings"),
      (name: "Joe", age: 11, testWithLongColumn: 1.1,         anotherCol: "short"),
    ]
    DF.fromSeq(data1).show(stdoutDummy)
    let data2 = @[
      (1, 1, 1),
      (2, 2, 2),
    ]
    DF.fromSeq(data2).show(stdoutDummy)
    let data3 = @[
      (f32: 0f32, f64: 0f64, i8: 0i8, i16: 0i16, i32: 0i32, i64: 0i64),
      (f32: 1f32, f64: 1f64, i8: 1i8, i16: 1i16, i32: 1i32, i64: 1i64),
    ]
    DF.fromSeq(data3).show(stdoutDummy)

  test "Custom width":
    var s = newStringStream()
    let data1 = @[
      (name: "Bob", age: 99, testWithLongColumn: 1.112341975, anotherCol: "with very long strings"),
      (name: "Joe", age: 11, testWithLongColumn: 1.1,         anotherCol: "short"),
    ]
    DF.fromSeq(data1).show(s, width = 12)
    s.setPosition(0)
    check: s.readAll() == """+--------------+--------------+--------------+--------------+
      | name         |          age | testWithLon… | anotherCol   |
      +--------------+--------------+--------------+--------------+
      | Bob          |           99 |  1.112341975 | with very l… |
      | Joe          |           11 |          1.1 | short        |
      +--------------+--------------+--------------+--------------+
    """.unindent()
