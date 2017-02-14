import future
import strutils
import math

import nimdata_utils
import nimdata_schema_parser


UnitTestSuite("Schema Parser"):
  test "skipPastSep -- empty":
    var i = 0
    var hitEnd = false
    let s = "test"
    skipPastSep(s, i, hitEnd, ';')
    check i == s.len
    check hitEnd == true

  test "skipPastSep -- regular":
    var i = 0
    var hitEnd = false
    let s = "hello;world;test"
    skipPastSep(s, i, hitEnd, ';')
    check s[i] == 'w'
    skipPastSep(s, i, hitEnd, ';')
    check s[i] == 't'
    skipPastSep(s, i, hitEnd, ';')
    check i == s.len
    check hitEnd == true

  test "skipPastSep -- pure separators":
    var i = 0
    var hitEnd = false
    let s = ";;;"
    skipPastSep(s, i, hitEnd, ';')
    check s[i] == ';'
    skipPastSep(s, i, hitEnd, ';')
    check s[i] == ';'
    skipPastSep(s, i, hitEnd, ';')
    check i == s.len
    check hitEnd == false
    skipPastSep(s, i, hitEnd, ';')
    check i == s.len
    check hitEnd == true

  test "Mixed columns":
    const schema = [
      col(StrCol, "columnA"),
      col(IntCol, "columnB"),
      col(FloatCol, "columnC"),
    ]
    let parser = schemaParser(schema, ';')
    let result = parser("1;2;3.5")
    check result == (columnA: "1", columnB: 2i64, columnC: 3.5)

  test "Mixed columns (different separator)":
    const schema = [
      col(StrCol, "columnA"),
      col(IntCol, "columnB"),
      col(FloatCol, "columnC"),
    ]
    let parser1 = schemaParser(schema, ',')
    let parser2 = schemaParser(schema, sep=',')
    check parser1("1,2,3.5") == (columnA: "1", columnB: 2i64, columnC: 3.5)
    check parser2("1,2,3.5") == (columnA: "1", columnB: 2i64, columnC: 3.5)

  # ---------------------------------------------------------------------------
  # string
  # ---------------------------------------------------------------------------

  test "Pure string column (1)":
    const schema = [
      col(StrCol, "columnA"),
    ]
    let parser = schemaParser(schema, ';')
    check:
      parser("hello") == (columnA: "hello")
      parser("") == (columnA: "")
      parser(" ") == (columnA: " ")
      parser(";") == (columnA: "") # do we want to support this or error?
      parser(" ;") == (columnA: " ") # do we want to support this or error?

  test "Pure string column (2)":
    const schema = [
      col(StrCol, "columnA"),
      col(StrCol, "columnB"),
    ]
    let parser = schemaParser(schema, ';')
    check:
      parser("hello;world") == (columnA: "hello", columnB: "world")

      parser(";world") == (columnA: "", columnB: "world")
      parser("hello;") == (columnA: "hello", columnB: "")
      parser(";") == (columnA: "", columnB: "")

      parser(" ;world") == (columnA: " ", columnB: "world")
      parser("hello; ") == (columnA: "hello", columnB: " ")
      parser(" ; ") == (columnA: " ", columnB: " ")

  test "Pure string column (3)":
    const schema = [
      col(StrCol, "columnA"),
      col(StrCol, "columnB"),
      col(StrCol, "columnC"),
    ]
    let parser = schemaParser(schema, ';')
    check:
      parser("hello;world;2") == (columnA: "hello", columnB: "world", columnC: "2")
      parser(";;;") == (columnA: "", columnB: "", columnC: "")
      parser(" ; ; ") == (columnA: " ", columnB: " ", columnC: " ")

  # ---------------------------------------------------------------------------
  # int
  # ---------------------------------------------------------------------------

  test "Pure int column (1)":
    const schema = [
      col(IntCol, "columnA"),
    ]
    let parser = schemaParser(schema, ';')
    check:
      parser("0") == (columnA: 0i64)
      parser("+0") == (columnA: 0i64)
      parser("-0") == (columnA: 0i64)
      parser(" 0") == (columnA: 0i64)
      parser("0 ") == (columnA: 0i64)
      parser("0;") == (columnA: 0i64)
      parser("+123467890") == (columnA: +123467890i64)
      parser("-123467890") == (columnA: -123467890i64)
      parser("0042") == (columnA: 42i64)

  test "Pure int column (2)":
    const schema = [
      col(IntCol, "columnA"),
      col(IntCol, "columnB"),
    ]
    let parser = schemaParser(schema, ';')
    check:
      parser("0;0") == (columnA: 0i64, columnB: 0i64)
      parser(" 0 ; 0 ") == (columnA: 0i64, columnB: 0i64)

  test "Pure int column (3)":
    const schema = [
      col(IntCol, "columnA"),
      col(IntCol, "columnB"),
      col(IntCol, "columnC"),
    ]
    let parser = schemaParser(schema, ';')
    check:
      parser("0;0;0") == (columnA: 0i64, columnB: 0i64, columnC: 0i64)

  # ---------------------------------------------------------------------------
  # float
  # ---------------------------------------------------------------------------

  test "Pure float column (1)":
    const schema = [
      col(FloatCol, "columnA"),
    ]
    let parser = schemaParser(schema, ';')
    check:
      parser("1.2") == (columnA: 1.2)
      parser(" 1.2 ") == (columnA: 1.2)
      parser(" +1.2 ") == (columnA: +1.2)
      parser(" -1.2 ") == (columnA: -1.2)
      parser(" 123e3 ") == (columnA: 123e3)
      parser(" .0001 ") == (columnA: 0.0001)
      parser("1.") == (columnA: 1.0)

  test "Pure float column (2)":
    const schema = [
      col(FloatCol, "columnA"),
      col(FloatCol, "columnB"),
    ]
    let parser = schemaParser(schema, ';')
    check:
      parser("1.2;1.2") == (columnA: 1.2, columnB: 1.2)
      parser(" 1.2 ; 1.2 ") == (columnA: 1.2, columnB: 1.2)
      parser("1.;1.") == (columnA: 1.0, columnB: 1.0)
      parser(".1;.1") == (columnA: 0.1, columnB: 0.1)

  test "Pure float column (3)":
    const schema = [
      col(FloatCol, "columnA"),
      col(FloatCol, "columnB"),
      col(FloatCol, "columnC"),
    ]
    let parser = schemaParser(schema, ';')
    check:
      parser("1.2;1.3;1.4") == (columnA: 1.2, columnB: 1.3, columnC: 1.4)

