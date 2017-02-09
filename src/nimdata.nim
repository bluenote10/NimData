import times
import typetraits
import strutils
import sequtils
import future
import macros
import random

import os
import browsers

import nimdata_schema_parser
export nimdata_schema_parser.Column
export nimdata_schema_parser.ColKind
export nimdata_schema_parser.col
export nimdata_schema_parser.schema_parser

import nimdata_html

type
  DataFrame*[T] = ref object of RootObj

  CachedDataFrame*[T] = ref object of DataFrame[T]
    data: seq[T]

  MappedDataFrame*[T, U] = ref object of DataFrame[T]
    orig: DataFrame[T]
    mapper: proc(x: T): U

  FilteredDataFrame*[T] = ref object of DataFrame[T]
    orig: DataFrame[T]
    f: proc(x: T): bool

  RangeDataFrame*[T] = ref object of DataFrame[T]
    lo, hi: int
  #[
  FileRowsDataFrame*[T] = ref object of DataFrame[T]
    filename: string
    dummy: T
  ]#


type
  DataFrameContext* = object

let
  DF* = DataFrameContext()

proc fromSeq*[T](dfc: DataFrameContext, data: seq[T]): DataFrame[T] =
  result = CachedDataFrame[T](data: data)

#[
# can't add this, because of "invalid declaration order" for `iter`
proc fromRange*(dfc: DataFrameContext, lo: int, hi: int): DataFrame[int] =
  result = RangeDataFrame[int](lo: lo, hi: hi)
]#

#[
proc newFileRowsDataFrame*(filename: string): DataFrame[string] =
  result = FileRowsDataFrame[string](filename: filename)
]#

# -----------------------------------------------------------------------------
# Transformations
# -----------------------------------------------------------------------------

method map*[T, U](df: DataFrame[T], f: proc(x: T): U): DataFrame[U] {.base.} =
  result = MappedDataFrame[T, U](orig: df, mapper: f)

method filter*[T](df: DataFrame[T], f: proc(x: T): bool): DataFrame[T] {.base.} =
  result = FilteredDataFrame[T](orig: df, f: f)

method sample*[T](df: DataFrame[T], probability: float): DataFrame[T] {.base.} =
  proc filter(x: T): bool = probability > random(1.0)
  result = FilteredDataFrame[T](orig: df, f: filter)

# -----------------------------------------------------------------------------
# Iterators
# -----------------------------------------------------------------------------

# not sure why I need this -- I actually store the iterator in a variable already
iterator toIterBugfix[T](closureIt: iterator(): T): T {.inline.} =
  for x in closureIt():
    yield x

method iter*[T](df: DataFrame[T]): (iterator(): T) {.base.} =
  echo df.type.name
  raise newException(IOError, "unimplemented iter")

method iter*[T](df: CachedDataFrame[T]): (iterator(): T) =
  result = iterator(): T =
    for x in df.data:
      yield x

method iter*[T, U](df: MappedDataFrame[T, U]): (iterator(): U) =
  result = iterator(): U =
    var it = df.orig.iter()
    for x in toIterBugfix(it):
      yield df.mapper(x)

method iter*[T](df: FilteredDataFrame[T]): (iterator(): T) =
  result = iterator(): T =
    var it = df.orig.iter()
    for x in toIterBugfix(it):
      if df.f(x):
        yield x

method iter*[T](df: RangeDataFrame[T]): (iterator(): T) =
  result = iterator(): T =
    for i in df.lo .. <df.hi:
      yield i

#[
method iter*[T](df: FileRowsDataFrame[T]): (iterator(): string) =
  result = iterator(): string =
    var f = open(df.filename, bufSize=8000)
    var res = TaintedString(newStringOfCap(80))
    while f.readLine(res):
      yield res
    close(f)
]#

# -----------------------------------------------------------------------------
# Actions
# -----------------------------------------------------------------------------

method collect*[T](df: DataFrame[T]): seq[T] =
  result = newSeq[T]()
  let it = df.iter()
  for x in it():
    result.add(x)

#[
method collect*[T](df: DataFrame[T]): seq[T] {.base.} =
  raise newException(IOError, "unimplemented")

method collect*[T](df: CachedDataFrame[T]): seq[T] =
  result = df.data

method collect*[S, T](df: MappedDataFrame[S, T]): seq[T] =
  result = newSeq[T]()
  let it = df.orig.iter()
  for x in it():
    result.add(df.mapper(x))

method collect*[T](df: FilteredDataFrame[T]): seq[T] =
  result = newSeq[T]()
  let it = df.orig.iter()
  for x in it():
    if df.f(x):
      result.add(x)

method collect*(df: FileRowsDataFrame): seq[string] =
  result = newSeq[string]()
  var f = open(df.filename, bufSize=8000)
  var res = TaintedString(newStringOfCap(80))
  while f.readLine(res):
    result.add(res)
  close(f)
]#

method count*[T](df: DataFrame[T]): int =
  result = 0
  let it = df.iter()
  for x in it():
    result += 1

method cache*[T](df: DataFrame[T]): DataFrame[T] =
  let data = df.collect()
  result = CachedDataFrame[T](data: data)


#[
# Even without calling any of these methods, the compiler thinks T is a string,
# resulting in errors like:
#
# Error: type mismatch: got (float, string)
# but expected one of:
# proc `+=`[T: SomeOrdinal | uint | uint64](x: var T; y: T)
# proc `+=`[T: float | float32 | float64](x: var T; y: T)
# proc `+=`(t: var Time; ti: TimeInterval)
#
# or:
#
# Error: type mismatch: got (typedesc[string])
# but expected one of:
# proc high[T](x: T): T
#
# How can I avoid that?

method mean*[T](df: DataFrame[T]): float =
  result = 0
  var count = 0
  let it = df.iter()
  for x in it():
    count += 1
    result += x
  result /= count

method min*[T](df: DataFrame[T]): T =
  result = T.high
  let it = df.iter()
  for x in it():
    if x < result:
      result = x

method max*[T](df: DataFrame[T]): T =
  result = T.low
  let it = df.iter()
  for x in it():
    if x > result:
      result = x
]#


proc toCsv*[T: tuple|object](df: DataFrame[T], filename: string, sep: char = ';') =
  var file = open(filename, fmWrite)
  defer: file.close()

  var dummy: T
  var i = 0

  for field, _ in dummy.fieldPairs(): # TODO: solve without dummy instance; report bug: SIGSEGV for dummy.fields()
    if i > 0:
      file.write(sep)
    file.write(field)
    i += 1
  file.write("\n")

  let it = df.iter()
  for x in it():
    i = 0
    for field, value in x.fieldPairs():
      if i > 0:
        file.write(sep)
      file.write(value)
      i += 1
    file.write("\n")


proc toHtml*[T: tuple|object](df: DataFrame[T], filename: string) =
  var tableStr = ""
  let it = df.iter()

  tableStr &= "<thead>\n"
  tableStr &= "<tr>"
  var dummy: T
  for field, _ in dummy.fieldPairs(): # TODO: solve without dummy instance; report bug: SIGSEGV for dummy.fields()
    tableStr &= "<th>"
    tableStr &= field
    tableStr &= "</th>"
  tableStr &= "</tr>\n"
  tableStr &= "</thead>\n"

  tableStr &= "<body>\n"
  for x in it():
    tableStr &= "<tr>"
    for field, value in x.fieldPairs():
      tableStr &= "<td>"
      tableStr &= value
      tableStr &= "</td>"
    tableStr &= "</tr>\n"
  tableStr &= "<tbody>\n"

  var html = htmlTemplate.replace("----table-data----", tableStr)
  var file = open(filename, fmWrite)
  file.write(html)
  file.close()


proc show*[T: tuple|object](df: DataFrame[T]) =
  let filename = getTempDir() / "table.html"
  df.toHtml(filename)
  openDefaultBrowser(filename)



