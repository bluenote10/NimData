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
    orig: DataFrame[U]
    f: proc(x: U): T

  MappedIndexDataFrame*[T, U] = ref object of DataFrame[T]
    orig: DataFrame[U]
    f: proc(i: int, x: U): T

  FilteredDataFrame*[T] = ref object of DataFrame[T]
    orig: DataFrame[T]
    f: proc(x: T): bool

  FilteredIndexDataFrame*[T] = ref object of DataFrame[T]
    orig: DataFrame[T]
    f: proc(i: int, x: T): bool

  RangeDataFrame*[T] = ref object of DataFrame[T]
    lo, hi: int

type
  DataFrameContext* = object

let
  DF* = DataFrameContext()
    ## Currently this constant is purely used for scoping,
    ## allowing to write expressions like ``DF.fromFile(...)``
    ## or ``DF.fromSeq(...)``. Eventually this might be used
    ## to store general context configuration.

proc fromSeq*[T](dfc: DataFrameContext, data: seq[T]): DataFrame[T] =
  ## Constructs a data frame from a sequence.
  result = CachedDataFrame[T](data: data)


# -----------------------------------------------------------------------------
# Transformations
# -----------------------------------------------------------------------------

method map*[T, U](df: DataFrame[U], f: proc(x: U): T): DataFrame[T] {.base.} =
  ## Transforms a ``DataFrame[T]`` into a ``DataFrame[U]`` by applying a
  ## mapping function ``f``.
  result = MappedDataFrame[T, U](orig: df, f: f)

method mapWithIndex*[T, U](df: DataFrame[U], f: proc(i: int, x: U): T): DataFrame[T] {.base.} =
  ## Transforms a ``DataFrame[T]`` into a ``DataFrame[U]`` by applying a
  ## mapping function ``f``.
  result = MappedIndexDataFrame[T, U](orig: df, f: f)

method filter*[T](df: DataFrame[T], f: proc(x: T): bool): DataFrame[T] {.base.} =
  ## Filters a data frame by applying a filter function ``f``.
  result = FilteredDataFrame[T](orig: df, f: f)

method filterWithIndex*[T](df: DataFrame[T], f: proc(i: int, x: T): bool): DataFrame[T] {.base.} =
  ## Filters a data frame by applying a filter function ``f``.
  result = FilteredIndexDataFrame[T](orig: df, f: f)

method take*[T](df: DataFrame[T], n: int): DataFrame[T] {.base.} =
  ## Selects the first `n` rows of a data frame.
  proc filter(i: int, x: T): bool = i < n
  result = FilteredIndexDataFrame[T](orig: df, f: filter)

method drop*[T](df: DataFrame[T], n: int): DataFrame[T] {.base.} =
  ## Discards the first `n` rows of a data frame.
  proc filter(i: int, x: T): bool = i >= n
  result = FilteredIndexDataFrame[T](orig: df, f: filter)

method sample*[T](df: DataFrame[T], probability: float): DataFrame[T] {.base.} =
  ## Filters a data frame by applying Bernoulli sampling with the specified
  ## sampling ``probability``.
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
      yield df.f(x)

method iter*[T, U](df: MappedIndexDataFrame[T, U]): (iterator(): U) =
  result = iterator(): U =
    var i = 0
    var it = df.orig.iter()
    for x in toIterBugfix(it):
      yield df.f(i, x)
      i += 1

method iter*[T](df: FilteredDataFrame[T]): (iterator(): T) =
  result = iterator(): T =
    var it = df.orig.iter()
    for x in toIterBugfix(it):
      if df.f(x):
        yield x

method iter*[T](df: FilteredIndexDataFrame[T]): (iterator(): T) =
  result = iterator(): T =
    var i = 0
    var it = df.orig.iter()
    for x in toIterBugfix(it):
      if df.f(i, x):
        yield x
      i += 1

method iter*[T](df: RangeDataFrame[T]): (iterator(): T) =
  result = iterator(): T =
    for i in df.lo .. <df.hi:
      yield i

# -----------------------------------------------------------------------------
# Actions
# -----------------------------------------------------------------------------

method collect*[T](df: DataFrame[T]): seq[T] {.base.} =
  ## Collects the content of a ``DataFrame[T]`` and returns it as ``seq[T]``.
  result = newSeq[T]()
  let it = df.iter()
  for x in it():
    result.add(x)

method collect*[T](df: CachedDataFrame[T]): seq[T] =
  ## Specialized implementation
  result = df.data


proc count*[T](df: DataFrame[T]): int = # TODO: want base method?
  ## Iterates over a data frame, and returns its length
  result = 0
  let it = df.iter()
  for x in it():
    result += 1


proc cache*[T](df: DataFrame[T]): DataFrame[T] = # TODO: want base method?
  ## Executes all chained operations on a data frame and returns
  ## a new data frame which is cached in memory. This will speed
  ## up subsequent operations on the data frame, and is useful
  ## when you have to perform multiple operation on the same
  ## data. However, make sure that you have enough memory to
  ## cache the input data.
  let data = df.collect()
  result = CachedDataFrame[T](data: data)


# When using methods instead of proces, even without calling any of them,
# the compiler thinks T is a string, resulting in errors like:
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

proc mean*[T](df: DataFrame[T]): float =
  ## Computes the mean of a data frame of numerical type ``T``.
  result = 0
  var count = 0
  let it = df.iter()
  for x in it():
    count += 1
    result += x.float
  result /= count.float

proc min*[T](df: DataFrame[T]): T =
  ## Computes the minimum of a data frame of numerical type ``T``.
  result = high(T)
  let it = df.iter()
  for x in it():
    if x < result:
      result = x

proc max*[T](df: DataFrame[T]): T =
  ## Computes the maximum of a data frame of numerical type ``T``.
  result = low(T)
  let it = df.iter()
  for x in it():
    if x > result:
      result = x


proc toCsv*[T: tuple|object](df: DataFrame[T], filename: string, sep: char = ';') =
  ## Store the data frame in a CSV
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
  ## Store the data frame in an HTML providing a simple table view.
  ## The current implementation uses simple static HTML, so make
  ## sure that your data frame is filtered down to a reasonable
  ## size.
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


proc openInBrowser*[T: tuple|object](df: DataFrame[T]) =
  ## Opens a table view of the data frame in the default browser.
  let filename = getTempDir() / "table.html"
  df.toHtml(filename)
  openDefaultBrowser(filename)


# -----------------------------------------------------------------------------
# Specialized DataFrame types
# (definition down here because of https://github.com/nim-lang/Nim/issues/5325)
# -----------------------------------------------------------------------------

type
  FileRowsDataFrame* = ref object of DataFrame[string]
    filename: string
    hasHeader: bool

proc fromFile*(dfc: DataFrameContext, filename: string, hasHeader: bool = true): DataFrame[string] =
  ## Constructs a data frame from a file.
  result = FileRowsDataFrame(
    filename: filename,
    hasHeader: hasHeader
  )

method iter*(df: FileRowsDataFrame): (iterator(): string) =
  result = iterator(): string =
    var f = open(df.filename, bufSize=8000)
    var res = TaintedString(newStringOfCap(80))
    if df.hasHeader:
      discard f.readLine(res)
    while f.readLine(res):
      yield res
    close(f)
