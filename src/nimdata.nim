## NimData's core data type is a generic ``DataFrame[T]``. The methods
## of a data frame can be categorized into generalizations
## of the Map/Reduce concept:
##
## - **Transformations**: Operations like ``map`` or ``filter`` transform one data
##   frame into another. Transformations are lazy and can be chained. They will only
##   be executed once an action is called.
## - **Actions**: Operations like ``count``, ``min``, ``max``, ``sum``, ``reduce``, ``fold``, ``collect``, or ``show``
##   perform an aggregation of a data frame, and trigger the processing pipeline.
##
## NimData is structured into the following submodules:
##
## - `nimdata/schema_parser <nimdata/schema_parser.html>`_ containing functions/macros for
##   static schema parsing.
## - `nimdata/tuples <nimdata/tuples.html>`_ containing functions/macros for transforming
##   tuples.
## - `nimdata/utils <nimdata/utils.html>`_ containing miscellaneous utils.
##
## This main module re-exports some symbols of these modules for convenience, so that
## ``import nimdata`` is sufficient in most cases.
##

when (NimMajor, NimMinor, NimPatch) > (0, 18, 0):
  import sugar
else:
  import future

import options

import typetraits
import macros

import strutils
import sequtils
import streams

import algorithm
import math
import random
import times
import os
import browsers

import sets
import tables

import nimdata/schema_parser
export schema_parser.strCol
export schema_parser.intCol
export schema_parser.floatCol
export schema_parser.dateCol
export schema_parser.schema_parser

import nimdata/tuples
export tuples.mergeTuple
export tuples.projectTo
export tuples.projectAway
export tuples.addField
export tuples.addFields

export SortOrder
export `=>`

import nimdata/io_gzip
import nimdata/html
import nimdata/utils


type
  DataFrame*[T] = ref object of RootObj

  CachedDataFrame[T] = ref object of DataFrame[T]
    data: seq[T]

  MappedDataFrame[U, T] = ref object of DataFrame[T]
    orig: DataFrame[U]
    f: proc(x: U): T {.locks: 0.}

  MappedIndexDataFrame[U, T] = ref object of DataFrame[T]
    orig: DataFrame[U]
    f: proc(i: int, x: U): T {.locks: 0.}

  FilteredDataFrame[T] = ref object of DataFrame[T]
    orig: DataFrame[T]
    f: proc(x: T): bool {.locks: 0.}

  FilteredIndexDataFrame[T] = ref object of DataFrame[T]
    orig: DataFrame[T]
    f: proc(i: int, x: T): bool {.locks: 0.}

  FlatMappedSeqDataFrame[U, T] = ref object of DataFrame[T]
    orig: DataFrame[U]
    f: proc(x: U): seq[T] {.locks: 0.}

  FlatMappedDataFrame[U, T] = ref object of DataFrame[T]
    orig: DataFrame[U]
    fIter: proc(x: U): (iterator(): T) {.locks: 0.}

  UniqueDataFrame[T] = ref object of DataFrame[T]
    orig: DataFrame[T]
    seen: HashSet[T]

  ValueCountsDataFrame[T] = ref object of DataFrame[tuple[key: T, count: int]]
    orig: DataFrame[T]
    counts: Table[T, int]

  GroupByReduceDataFrame[T, K, U] = ref object of DataFrame[U]
    keyFunc: proc(x: T): K {.locks: 0.}
    reduceFunc: proc(key: K, df: DataFrame[T]): U {.locks: 0.}
    orig: DataFrame[T]
    computed: bool
    data: Table[K, seq[T]]

  SortDataFrame[T, U] = ref object of DataFrame[T]
    orig: DataFrame[T]
    computed: bool
    data: Option[seq[T]]
    f: proc(x: T): U {.locks: 0.}
    order: SortOrder

  #[
  JoinDataFrame[O: static[seq[string]], A, B, C] = ref object of DataFrame[C]
    origA: DataFrame[A]
    origB: DataFrame[B]
  ]#

  JoinThetaDataFrame[A, B, C] = ref object of DataFrame[C]
    origA: DataFrame[A]
    origB: DataFrame[B]
    cmpFunc: proc(a: A, b: B): bool {.locks: 0.}
    projectFunc: proc(a: A, b: B): C {.locks: 0.}
    computed: bool
    dataB: Option[seq[B]]

  JoinEquiDataFrame[A, B, C, D] = ref object of DataFrame[D]
    origA: DataFrame[A]
    origB: DataFrame[B]
    hashFuncA: proc(a: A): C {.locks: 0.}
    hashFuncB: proc(b: B): C {.locks: 0.}
    projectFunc: proc(a: A, b: B): D {.locks: 0.}
    computed: bool
    dataB: Table[C, seq[B]]


# -----------------------------------------------------------------------------
# Transformations
# -----------------------------------------------------------------------------

proc map*[U, T](df: DataFrame[U], f: proc(x: U): T): DataFrame[T] =
  ## Transforms a ``DataFrame[U]`` into a ``DataFrame[T]`` by applying a
  ## mapping function ``f``.
  result = MappedDataFrame[U, T](orig: df, f: f)

proc mapWithIndex*[U, T](df: DataFrame[U], f: proc(i: int, x: U): T): DataFrame[T] =
  ## Transforms a ``DataFrame[U]`` into a ``DataFrame[T]`` by applying a
  ## mapping function ``f``.
  result = MappedIndexDataFrame[U, T](orig: df, f: f)

proc filter*[T](df: DataFrame[T], f: proc(x: T): bool): DataFrame[T] =
  ## Filters a data frame by applying a filter function ``f``.
  result = FilteredDataFrame[T](orig: df, f: f)

proc filterWithIndex*[T](df: DataFrame[T], f: proc(i: int, x: T): bool): DataFrame[T] =
  ## Filters a data frame by applying a filter function ``f``.
  result = FilteredIndexDataFrame[T](orig: df, f: f)

proc take*[T](df: DataFrame[T], n: int): DataFrame[T] =
  ## Selects the first `n` rows of a data frame.
  proc filter(i: int, x: T): bool = i < n
  result = FilteredIndexDataFrame[T](orig: df, f: filter)

proc drop*[T](df: DataFrame[T], n: int): DataFrame[T] =
  ## Discards the first `n` rows of a data frame.
  proc filter(i: int, x: T): bool = i >= n
  result = FilteredIndexDataFrame[T](orig: df, f: filter)

proc sample*[T](df: DataFrame[T], probability: float): DataFrame[T] =
  ## Filters a data frame by applying Bernoulli sampling with the specified
  ## sampling ``probability``.
  when (NimMajor, NimMinor, NimPatch) > (0, 18, 0):
    proc filter(x: T): bool = probability > rand(1.0)
  else:
    proc filter(x: T): bool = probability > random(1.0)
  result = FilteredDataFrame[T](orig: df, f: filter)

proc flatMap*[U, T](df: DataFrame[U], f: proc(x: U): seq[T]): DataFrame[T] =
  ## Transforms a ``DataFrame[U]`` into a ``DataFrame[T]`` by applying ``f``
  ## to each element of the input data frame, and inserting the elements of
  ## the output ``seq[T]`` into the result data frame.
  result = FlatMappedSeqDataFrame[U, T](orig: df, f: f)

proc flatMap*[U, T](df: DataFrame[U], fIter: proc(x: U): (iterator(): T)): DataFrame[T] =
  ## Transforms a ``DataFrame[U]`` into a ``DataFrame[T]`` by applying an
  ## iterator ``fIter`` to each element of the input data frame.
  result = FlatMappedDataFrame[U, T](orig: df, fIter: fIter)

proc unique*[T](df: DataFrame[T]): DataFrame[T] =
  ## Returns a data frame, which consists of the unique values of the input
  ## data frame. Note that the memory requirement is linear in the number
  ## of unique values, so use with care. Type T must provide a hash function
  ## with signature ``hash(x: T): Hash`` (see
  ## `hashes <https://nim-lang.org/docs/hashes.html>`_ documentation).
  result = UniqueDataFrame[T](orig: df, seen: initSet[T]())

proc valueCounts*[T](df: DataFrame[T]): DataFrame[tuple[key: T, count: int]] =
  ## Returns a data frame, which consists of the unique values and theirs
  ## respective counts. Thus, the type of the resulting data frame is
  ## a tuple of ``(key: T, count: int)``. Note that the memory requirement
  ## is linear in the number of unique values, so use with care.
  ## Type T must provide a hash function with signature ``hash(x: T): Hash``
  ## (see `hashes <https://nim-lang.org/docs/hashes.html>`_ documentation).
  result = ValueCountsDataFrame[T](orig: df, counts: initTable[T, int]())

proc genericIdentity[T](x: T): T = x

proc sort*[T, U](df: DataFrame[T], f: proc(x: T): U, order: SortOrder = SortOrder.Ascending): DataFrame[T] =
  ## Returns a sorted data frame, where ``f`` defines the sort key.
  ## Note: The current implementation does not yet use a spill-to-disk,
  ## so the data frame must fit into memory.
  result = SortDataFrame[T, U](
    orig: df,
    computed: false,
    data: none seq[T],
    f: f,
    order: order,
  )

proc sort*[T](df: DataFrame[T], order: SortOrder = SortOrder.Ascending): DataFrame[T] =
  ## Returns a sorted data frame. The current implementation does not yet
  ## use a spill-to-disk, so the data frame must fit into memory.
  result = SortDataFrame[T, T](
    orig: df,
    computed: false,
    data: none(seq[T]),
    f: genericIdentity[T],
    order: order,
  )

proc groupBy*[T, K, U](df: DataFrame[T], keyFunc: proc(x: T): K, reduceFunc: proc(key: K, df: DataFrame[T]): U): DataFrame[U] =
  ## Groups a data frame according to ``keyFunc`` and applies
  ## ``reduceFunc`` to each group.
  result = GroupByReduceDataFrame[T, K, U](
    keyFunc: keyFunc,
    reduceFunc: reduceFunc,
    orig: df,
    computed: false,
    data: initTable[K, seq[T]]()
  )

#[
proc join*[A, B](dfA: DataFrame[A], dfB: DataFrame[B], on: static[openarray[string]]): auto =
  result = JoinDataFrame[on, A, B, determineType(A, B, on)](
    origA: dfA,
    origB: dfB
  )
]#

proc joinTheta*[A, B, C](dfA: DataFrame[A],
                    dfB: DataFrame[B],
                    cmpFunc: (a: A, b: B) -> bool,
                    projectFunc: (a: A, b: B) -> C): DataFrame[C] =
  ## Performs on inner join of two data frames based on the given
  ## ``cmpFunc``. The result can be arbitrarily merged using the
  ## ``projectFunc``. When working with named tuples, the macro
  ## `mergeTuple <nimdata/tuples.html#mergeTuple>`_ can be used as
  ## a convenient way to merge the fields of tuple `A` and `B`.
  ## The current implementation caches ``dfB`` internally. Thus,
  ## when joining a large and a small data frame, make sure that
  ## the left (``dfA``) is the large one and the right (``dfB``)
  ## is the smaller one.
  result = JoinThetaDataFrame[A, B, C](
    origA: dfA,
    origB: dfB,
    cmpFunc: cmpFunc,
    projectFunc: projectFunc,
    computed: false,
    dataB: none seq[B]
  )

proc joinEqui*[A, B, C, D](dfA: DataFrame[A],
                           dfB: DataFrame[B],
                           hashFuncA: (a: A) -> C,
                           hashFuncB: (b: B) -> C,
                           projectFunc: (a: A, b: B) -> D): DataFrame[D] =
  ## Performs on inner join of two data frames based on the given
  ## ``hashFuncA`` and ``hashFuncB``. The result can be arbitrarily merged using the
  ## ``projectFunc``. When working with named tuples, the macro
  ## `mergeTuple <nimdata/tuples.html#mergeTuple>`_ can be used as
  ## a convenient way to merge the fields of tuple `A` and `B`.
  ## The current implementation caches ``dfB`` internally. Thus,
  ## when joining a large and a small data frame, make sure that
  ## the left (``dfA``) is the large one and the right (``dfB``)
  ## is the smaller one.
  result = JoinEquiDataFrame[A, B, C, D](
    origA: dfA,
    origB: dfB,
    hashFuncA: hashFuncA,
    hashFuncB: hashFuncB,
    projectFunc: projectFunc,
    computed: false,
    dataB: initTable[C, seq[B]]()
  )

macro join*[A, B](dfA: DataFrame[A],
                  dfB: DataFrame[B],
                  on: untyped): untyped =

  if on.kind != nnkBracket:
    error "join expects a bracket on clause. Type is " & $on.kind

  let keyExprA = newPar()
  for field in on:
    let dotExpr = newDotExpr(ident "x", field)
    keyExprA.add(dotExpr)
  let keyFuncA = infix(ident "x", "=>", keyExprA)

  let keyExprB = newPar()
  for field in on:
    let dotExpr = newDotExpr(ident "x", field)
    keyExprB.add(dotExpr)
  let keyFuncB = infix(ident "x", "=>", keyExprB)

  var onAsString = newNimNode(nnkBracket)
  for field in on:
    onAsString.add(newStrLitNode($field))
  var projectFunc = quote do:
    (a, b) => mergeTuple(a, b, `onAsString`)
  # get first child to unwrap the nnkStmtList
  projectFunc = projectFunc[0]

  result = newCall(
    bindSym "joinEqui",
    dfA,
    dfB,
    keyFuncA,
    keyFuncB,
    projectFunc
  )

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

method iter*[T, U](df: FlatMappedSeqDataFrame[T, U]): (iterator(): U) =
  result = iterator(): U =
    var it = df.orig.iter()
    for x in toIterBugfix(it):
      for y in df.f(x):
        yield y

method iter*[T, U](df: FlatMappedDataFrame[T, U]): (iterator(): U) =
  result = iterator(): U =
    var it = df.orig.iter()
    for x in toIterBugfix(it):
      var subIter = df.fIter(x)
      for y in toIterBugfix(subIter):
        yield y

method iter*[T](df: UniqueDataFrame[T]): (iterator(): T) =
  result = iterator(): T =
    var it = df.orig.iter()
    for x in toIterBugfix(it):
      if not df.seen.containsOrIncl(x):
        yield x

method iter*[T](df: ValueCountsDataFrame[T]): (iterator(): tuple[key: T, count: int]) =
  result = iterator(): tuple[key: T, count: int] =
    var it = df.orig.iter()
    for x in toIterBugfix(it):
      df.counts.mgetOrPut(x, 0) += 1
    for key, count in df.counts.pairs:
      yield (key: key, count: count)

method iter*[T, U](df: SortDataFrame[T, U]): (iterator(): T) =
  if not df.computed:
    df.data = some df.orig.collect()
    sort(
      get df.data,
      (x, y) => cmp(df.f(x), df.f(y)),
      df.order
    )
    df.computed = true

  result = iterator(): T =
    for x in get df.data:
      yield x

method iter*[T, K, U](df: GroupByReduceDataFrame[T, K, U]): (iterator(): U) =
  if not df.computed:
    var it = df.orig.iter()
    for x in toIterBugfix(it):
      let key = df.keyFunc(x)
      df.data.mgetOrPut(key, newSeq[T]()).add(x)
    df.computed = true

  result = iterator(): U =
    for key, values in df.data.pairs:
      let dfGroup = CachedDataFrame[T](data: values) # TODO: avoid copying?
      let reduced = df.reduceFunc(key, dfGroup)
      yield reduced

method iter*[A, B, C](df: JoinThetaDataFrame[A, B, C]): (iterator(): C) =
  if not df.computed:
    df.dataB = some df.origB.collect()
    df.computed = true

  result = iterator(): C =
    var it = df.origA.iter()
    for a in toIterBugfix(it):
      for b in get df.dataB:
        let matches = df.cmpFunc(a, b)
        if matches:
          yield df.projectFunc(a, b)

method iter*[A, B, C, D](df: JoinEquiDataFrame[A, B, C, D]): (iterator(): D) =
  if not df.computed:
    var it = df.origB.iter()
    for b in toIterBugfix(it):
      let key = df.hashFuncB(b)
      df.dataB.mgetOrPut(key, newSeq[B]()).add(b)
    df.computed = true

  result = iterator(): D =
    var it = df.origA.iter()
    for a in toIterBugfix(it):
      let key = df.hashFuncA(a)
      if key in df.dataB:
        for b in df.dataB[key]:
          yield df.projectFunc(a, b)

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

proc show*[T: not tuple](df: DataFrame[T], s: Stream = nil) =
  ## Prints the content of the data frame using generic to string conversion.
  ## If no stream is specified, the output is written to ``stdout``.

  var stream: Stream

  if s.isNil:
    stream = newFileStream(stdout)
  else:
    stream = s

  proc print(x: T) =
    stream.writeLine(x)

  df.forEach(print)

proc separatorRowIntercepted(sizes: seq[int], interceptor: char): string =
  result = newStringOfCap(30)
  result &= interceptor
  for i, size in sizes.pairs:
    result &= '-'.repeat(size + 2)
    result &= interceptor

proc show*[T: tuple](df: DataFrame[T], s: Stream = nil) =
  ## Prints the content of the data frame in the form of an ASCII table.
  ## If no stream is specified, the output is written to ``stdout``.
  var dummy: T
  var i = 0
  let fields = getFields(T)
  let sizes = 10.repeat(fields.len)
  var stream: Stream

  if s.isNil:
    stream = newFileStream(stdout)
  else:
    stream = s

  stream.writeLine(separatorRowIntercepted(sizes, '+'))

  var totalLineWidth = 0
  for field, value in dummy.fieldPairs:
    if i == 0:
      stream.write("| ")
      totalLineWidth += 2
    else:
      stream.write(" | ")
      totalLineWidth += 3
    when value is string:
      let strFormatted = field | -sizes[i]
    else:
      let strFormatted = field | +sizes[i]
    stream.write(fixedTruncateR(strFormatted, sizes[i]))
    totalLineWidth += sizes[i]
    i += 1
  stream.write(" |\n")
  totalLineWidth += 2

  stream.writeLine(separatorRowIntercepted(sizes, '+'))

  let it = df.iter()
  for x in it():
    i = 0
    for field, value in x.fieldPairs():
      if i == 0:
        stream.write("| ")
      else:
        stream.write(" | ")
      when value is string:
        let strFormatted = value | -sizes[i]
      else:
        let strFormatted = $value | +sizes[i]
      stream.write(fixedTruncateR(strFormatted, sizes[i]))
      i += 1
    stream.write(" |\n")

  stream.writeLine(separatorRowIntercepted(sizes, '+'))

# -----------------------------------------------------------------------------
# Actions (numerical)
# -----------------------------------------------------------------------------

proc sum*[T](df: DataFrame[T]): T =
  ## Computes the sum of a data frame of numerical type ``T``.
  let it = df.iter()
  for x in it():
    result += x

proc mean*[T: SomeNumber](df: DataFrame[T]): float =
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

proc median*[T: SomeNumber](df: DataFrame[T]): T =
  ## Computes the median of a data frame of numerical type ``T``.
  let dfSorted = df.sort()
  let it = dfSorted.iter()
  let values = toSeq(it())
  let n = values.len
  if n mod 2 == 0:
    return (values[n div 2] + values[(n div 2) - 1]) / 2
  else:
    return values[n div 2]

proc SS[T: SomeNumber](df: DataFrame[T]): float =
  ## Sum of squared deviations
  let c = df.mean()
  let it = df.iter()
  for x in it():
    result += pow(x.float - c, 2)

proc stdev*[T: SomeNumber](df: DataFrame[T], ddof = 0): float =
  ## Computes the standard deviation of a data frame of numerical type ``T``.
  ## Population standard deviation by default. Specify ``ddof=1`` to compute
  ## the sample standard deviation.
  let n = df.count()
  if n < 2:
    raise newException(ValueError, "stdev requires at least two data points")
  let pvar = df.SS() / (n - ddof).float
  result = sqrt(pvar)

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
      tableStr &= $value
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

# -----------------------------------------------------------------------------
# Range
type
  RangeDataFrame = ref object of DataFrame[int]
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
    for i in df.indexFrom..<df.indexUpto:
      yield i


# -----------------------------------------------------------------------------
# FileRows (uncompressed)
type
  FileRowsDataFrame = ref object of DataFrame[string]
    filename: string
    hasHeader: bool

method iter*(df: FileRowsDataFrame): (iterator(): string) =
  result = iterator(): string =
    var f = open(df.filename, bufSize=8000)
    var res = TaintedString(newStringOfCap(80))
    if df.hasHeader:
      discard f.readLine(res)
    while f.readLine(res):
      yield res
    close(f)

# -----------------------------------------------------------------------------
# FileRows (gzip)
type
  FileRowsGZipDataFrame = ref object of DataFrame[string]
    filename: string
    hasHeader: bool

method iter*(df: FileRowsGZipDataFrame): (iterator(): string) =
  result = iterator(): string =
    var stream = newGZipStream(df.filename)
    var res = TaintedString(newStringOfCap(80))
    if df.hasHeader:
      discard stream.readLine(res)
    while stream.readLine(res):
      yield res
    stream.close()


# -----------------------------------------------------------------------------
# Smart from file construction
type
  FileType* = enum
    Auto, RawText, GZip

proc fromFile*(dfc: DataFrameContext,
               filename: string,
               fileType: FileType = FileType.Auto,
               hasHeader: bool = true): DataFrame[string] =
  ## Constructs a data frame from a file, iterating the file
  ## line by line. By default the file type is inferred from
  ## the file name, but it can also be specified explicitly.

  proc hasSuffix(s, suffix: string): bool =
    s.toLowerAscii.endswith("." & suffix)

  case fileType
  of FileType.RawText:
    result = FileRowsDataFrame(
      filename: filename,
      hasHeader: hasHeader
    )
  of GZip:
    result = FileRowsGZipDataFrame(
      filename: filename,
      hasHeader: hasHeader
    )
  of FileType.Auto:
    if filename.hasSuffix("gz"):
      result = FileRowsGZipDataFrame(
        filename: filename,
        hasHeader: hasHeader
      )
    else:
      result = FileRowsDataFrame(
        filename: filename,
        hasHeader: hasHeader
      )
