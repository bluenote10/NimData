import times
import typetraits
import strutils
import sequtils
import future
import macros

import nimdata_schema_parser
export nimdata_schema_parser.Column
export nimdata_schema_parser.ColKind
export nimdata_schema_parser.col
export nimdata_schema_parser.schema_parser


macro debug*(n: varargs[typed]): untyped =
  result = newNimNode(nnkStmtList, n)
  for i in 0..n.len-1:
    add(result, newCall("write", newIdentNode("stdout"), toStrLit(n[i])))
    add(result, newCall("write", newIdentNode("stdout"), newStrLitNode(": ")))
    add(result, newCall("write", newIdentNode("stdout"), n[i]))
    if i != n.len-1:
      add(result, newCall("write", newIdentNode("stdout"), newStrLitNode(", ")))
  add(result, newCall("writeLine", newIdentNode("stdout"), newStrLitNode("")))

#[
macro printExpr(x: untyped): untyped =
  echo x.toStrLit
  result = quote do: discard
]#

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

  #[
  FileRowsDataFrame*[T] = ref object of DataFrame[T]
    filename: string
    dummy: T
  ]#


proc newCachedDataFrame*[T](data: seq[T]): DataFrame[T] =
  result = CachedDataFrame[T](data: data)

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

# -----------------------------------------------------------------------------
# Iterators
# -----------------------------------------------------------------------------

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
  result = newCachedDataFrame[T](data)




