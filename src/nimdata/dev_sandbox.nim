#[
-------------------------------------------------------------------------------
This file contains some internal dev notes/todos. Can be ignored...
-------------------------------------------------------------------------------
]#
import ../nimdata

when false:
  # would be nice to find a better solution than the echoGeneric
  # maybe by enhancing the upstream `=>`.
  const schema = [
    strCol("index"),
    strCol("homeTeam"),
    strCol("awayTeam"),
    intCol("homeGoals"),
    intCol("awayGoals"),
    intCol("round"),
    intCol("year"),
    dateCol("date", format="yyyy-MM-dd hh:mm:ss")
  ]
  DF.fromFile("examples/Bundesliga.csv")
    .map(schemaParser(schema, ','))
    .take(10)
    #.forEach(echo)                           # doesn't work unfortunately
    #.forEach(x => echo x)                    # doesn't work unfortunately
    #.forEach(proc (x: tuple): void = echo x) # this works
    .forEach(echoGeneric)                     # this works


when true:

  import macros

  macro typedescToType(tdesc: typed): untyped =
    # echo tdesc.repr               = T
    # echo tdesc.getType.repr       = typeDesc[tuple[string, string, string, ...]]
    # echo tdesc.getTypeImpl.repr   = typeDesc[tuple[index: string, homeTeam: string, ...]]
    # getTypeImpl(df.T) itself gives a typedesc[tuple[...]] so we need
    # can simply extract the type from the child at index 1.
    result = tdesc.getTypeImpl[1]

  template extractType*[T](df: DataFrame[T]): untyped =
    typedescToType(df.T)

  template inspectFields*[T](df: DataFrame[T]): untyped =
    var x: typedescToType(df.T)
    x

  const schema = [
    strCol("index"),
    strCol("homeTeam"),
    strCol("awayTeam"),
    intCol("homeGoals"),
    intCol("awayGoals"),
    intCol("round"),
    intCol("year"),
    dateCol("date", format="yyyy-MM-dd hh:mm:ss")
  ]
  let df = DF.fromFile("examples/Bundesliga.csv")
             .map(schemaParser(schema, ','))
             .map(x => x.projectAway(index))
             .map(x => addFields(x, goalDiff: x.homeGoals - x.awayGoals))

  type
    ResultType = extractType(df)

  discard inspectFields(df)
