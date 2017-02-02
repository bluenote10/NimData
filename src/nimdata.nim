import times
import typetraits
import strutils
import sequtils
import future
import macros

type
  Iter[T] = object

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
  DataFrame[T] = ref object of RootObj

  PersistedDataFrame[T] = ref object of DataFrame[T]
    data: seq[T]

  MappedDataFrame[T, U] = ref object of DataFrame[T]
    orig: DataFrame[T]
    mapper: proc(x: T): U

  FilteredDataFrame[T] = ref object of DataFrame[T]
    orig: DataFrame[T]
    f: proc(x: T): bool

proc newPersistedDataFrame[T](data: seq[T]): DataFrame[T] =
  result = PersistedDataFrame[T](data: data)

# -----------------------------------------------------------------------------
# Transformations
# -----------------------------------------------------------------------------

method map[T, U](df: DataFrame[T], f: proc(x: T): U): DataFrame[U] {.base.} =
  result = MappedDataFrame[T, U](orig: df, mapper: f)

method filter[T](df: DataFrame[T], f: proc(x: T): bool): DataFrame[T] {.base.} =
  result = FilteredDataFrame[T](orig: df, f: f)

# -----------------------------------------------------------------------------
# Iterators
# -----------------------------------------------------------------------------

iterator toIterBugfix[T](closureIt: iterator(): T): T {.inline.} =
  for x in closureIt():
    yield x

method iter[T](df: DataFrame[T]): (iterator(): T) {.base.} =
  raise newException(IOError, "unimplemented")

method iter[T](df: PersistedDataFrame[T]): (iterator(): T) =
  result = iterator(): T =
    for x in df.data:
      yield x

method iter[T, U](df: MappedDataFrame[T, U]): (iterator(): U) =
  result = iterator(): U =
    var it = df.orig.iter()
    for x in toIterBugfix(it):
      yield df.mapper(x)

method iter[T](df: FilteredDataFrame[T]): (iterator(): T) =
  result = iterator(): T =
    var it = df.orig.iter()
    for x in toIterBugfix(it):
      if df.f(x):
        yield x


# -----------------------------------------------------------------------------
# Actions
# -----------------------------------------------------------------------------

method collect[T](df: DataFrame[T]): seq[T] {.base.} =
  raise newException(IOError, "unimplemented")

method collect[T](df: PersistedDataFrame[T]): seq[T] =
  result = df.data

method collect[S, T](df: MappedDataFrame[S, T]): seq[T] =
  result = newSeq[T]()
  let it = df.orig.iter()
  for x in it():
    result.add(df.mapper(x))
  #for x in df.orig.



let data = newPersistedDataFrame[int](@[1, 2, 3])
let iter1 = data.iter()
for x in iter1():   # data.iter does not work, it is the ugly `(data.iter)()` compiles but inf-loops
  echo x
echo "data.collect() = ", data.collect()

#let mapped = map(data, x => x*2)
let mapped = data.map(x => x*2)
let iter2 = mapped.iter
for x in iter2():   # data.iter does not work, it is the ugly `(data.iter)()` compiles but inf-loops
  echo x

echo "mapped.collect() = ",
      mapped.collect()
echo "data.map(x => x*2).collect() = ",
      data.map(x => x*2).collect()
echo "data.map(x => x*2).map(x => x*2).collect() = ",
      data.map(x => x*2).map(x => x*2).collect()
echo "data.filter(x => x mod 2 == 1).map(x => x * 100).collect() = ",
      data.filter(x => x mod 2 == 1).map(x => x * 100).collect()

# printExpr(data.filter(x => x mod 2 == 1).map(x => x * 100).collect())







