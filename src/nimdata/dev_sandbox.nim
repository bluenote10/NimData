#[
-------------------------------------------------------------------------------
This file contains some internal dev notes/todos. Can be ignored...
-------------------------------------------------------------------------------
]#
import ../nimdata

block:
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
