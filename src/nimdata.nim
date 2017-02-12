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

  MappedDataFrame*[U, T] = ref object of DataFrame[T]
    orig: DataFrame[U]
    f: proc(x: U): T

  MappedIndexDataFrame*[U, T] = ref object of DataFrame[T]
    orig: DataFrame[U]
    f: proc(i: int, x: U): T

  FilteredDataFrame*[T] = ref object of DataFrame[T]
    orig: DataFrame[T]
    f: proc(x: T): bool

  FilteredIndexDataFrame*[T] = ref object of DataFrame[T]
    orig: DataFrame[T]
    f: proc(i: int, x: T): bool

# -----------------------------------------------------------------------------
# Transformations
# -----------------------------------------------------------------------------

method map*[U, T](df: DataFrame[U], f: proc(x: U): T): DataFrame[T] {.base.} =
  ## Transforms a ``DataFrame[U]`` into a ``DataFrame[T]`` by applying a
  ## mapping function ``f``.
  result = MappedDataFrame[U, T](orig: df, f: f)

method mapWithIndex*[U, T](df: DataFrame[U], f: proc(i: int, x: U): T): DataFrame[T] {.base.} =
  ## Transforms a ``DataFrame[U]`` into a ``DataFrame[T]`` by applying a
  ## mapping function ``f``.
  result = MappedIndexDataFrame[U, T](orig: df, f: f)

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

# -----------------------------------------------------------------------------
# Actions
# -----------------------------------------------------------------------------

proc count*[T](df: DataFrame[T]): int = # TODO: want base method?
  ## Iterates over a data frame, and returns its length
  result = 0
  let it = df.iter()
  for x in it():
    result += 1

proc reduce*[T](df: DataFrame[T], f: proc(a, b: T): T): T =
  ## Applies a reduce function ``f`` to the data frame following
  ## the pattern ``f( ... f(f(f(x[0], x[1]), x[2]), x[3]) ...)``.
  # TODO: better explanation
  let it = df.iter()
  result = it()
  for x in it():
    result = f(result, x)

proc fold*[U, T](df: DataFrame[U], init: T, f: proc(a: T, b: U): T): T =
  ## Applies a fold/aggregation function ``f`` to the data frame following
  ## the pattern ``f( ... f(f(f(init, x[0]), x[1]), x[2]) ...)``.
  # TODO: better explanation
  result = init
  let it = df.iter()
  for x in it():
    result = f(result, x)


proc cache*[T](df: DataFrame[T]): DataFrame[T] = # TODO: want base method?
  ## Executes all chained operations on a data frame and returns
  ## a new data frame which is cached in memory. This will speed
  ## up subsequent operations on the data frame, and is useful
  ## when you have to perform multiple operation on the same
  ## data. However, make sure that you have enough memory to
  ## cache the input data.
  let data = df.collect()
  result = CachedDataFrame[T](data: data)


proc forEach*[T](df: DataFrame[T], f: proc(x: T): void) =
  ## Applies a function ``f`` to all elements of a data frame.
  let it = df.iter()
  for x in it():
    f(x)


method collect*[T](df: DataFrame[T]): seq[T] {.base.} =
  ## Collects the content of a ``DataFrame[T]`` and returns it as ``seq[T]``.
  result = newSeq[T]()
  let it = df.iter()
  for x in it():
    result.add(x)

method collect*[T](df: CachedDataFrame[T]): seq[T] =
  ## Specialized implementation
  result = df.data

proc echoGeneric*[T](x: T) {.procvar.} =
  ## Convenience to allow ``df.forEach(echoGeneric)``
  echo x

# -----------------------------------------------------------------------------
# Actions (numerical)
# -----------------------------------------------------------------------------

proc sum*[T](df: DataFrame[T]): T =
  ## Computes the sum of a data frame of numerical type ``T``.
  let it = df.iter()
  for x in it():
    result += x

proc mean*[T](df: DataFrame[T]): float =
  ## Computes the mean of a data frame of numerical type ``T``.
  result = 0f
  var count = 0
  let it = df.iter()
  for x in it():
    count += 1
    result += x.float
  result /= count.float

proc min*[T](df: DataFrame[T]): T =
  ## Computes the minimum of a data frame of numerical type ``T``.
  when compiles(high(T)):
    result = high(T)
  else:
    result = +(Inf.T) # for floats
  let it = df.iter()
  for x in it():
    if x < result:
      result = x

proc max*[T](df: DataFrame[T]): T =
  ## Computes the maximum of a data frame of numerical type ``T``.
  when compiles(low(T)):
    result = low(T) # for ordinal types
  else:
    result = -(Inf.T) # for floats
  let it = df.iter()
  for x in it():
    if x > result:
      result = x

# -----------------------------------------------------------------------------
# Actions (IO)
# -----------------------------------------------------------------------------

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


type
  RangeDataFrame* = ref object of DataFrame[int]
    indexFrom, indexUpto: int

proc fromRange*(dfc: DataFrameContext, indexFrom: int, indexUpto: int): DataFrame[int] =
  ## Constructs a ``DataFrame[int]`` which iterates over the interval
  ## ``[indexFrom, indexUpto)``, i.e.,
  ## from ``indexFrom`` (inclusive) up to ``indexUpto`` (exclusive).
  result = RangeDataFrame(
    indexFrom: indexFrom,
    indexUpto: indexUpto
  )

proc fromRange*(dfc: DataFrameContext, indexUpto: int): DataFrame[int] =
  ## Constructs a ``DataFrame[int]`` which iterates over the interval
  ## ``[0, indexUpto)``, i.e.,
  ## from 0 (inclusive) up to ``indexUpto`` (exclusive).
  result = RangeDataFrame(
    indexFrom: 0,
    indexUpto: indexUpto
  )

method iter*(df: RangeDataFrame): (iterator(): int) =
  result = iterator(): int =
    for i in df.indexFrom .. <df.indexUpto:
      yield i


type
  FileRowsDataFrame* = ref object of DataFrame[string]
    filename: string
    hasHeader: bool

proc fromFile*(dfc: DataFrameContext, filename: string, hasHeader: bool = true): DataFrame[string] =
  ## Constructs a data frame from a text file, iterating
  ## the file line by line.
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
