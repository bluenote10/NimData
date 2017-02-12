#[
-------------------------------------------------------------------------------
This file contains some internal dev notes/todos. Can be ignored...
-------------------------------------------------------------------------------
]#
import future
import strutils
import nimdata
import nimdata_utils

block:
  # would be nice to find a better solution than the echoGeneric
  # maybe by enhancing the upstream `=>`.
  const schema = [
    col(StrCol, "index"),
    col(StrCol, "homeTeam"),
    col(StrCol, "awayTeam"),
    col(IntCol, "homeGoals"),
    col(IntCol, "awayGoals"),
    col(IntCol, "round"),
    col(IntCol, "year"),
    col(StrCol, "date") # TODO: proper timestamp parsing
  ]
  DF.fromFile("examples/Bundesliga.csv")
    .map(schemaParser(schema, ','))
    .take(10)
    #.forEach(echo)                           # doesn't work unfortunately
    #.forEach(x => echo x)                    # doesn't work unfortunately
    #.forEach(proc (x: tuple): void = echo x) # this works
    .forEach(echoGeneric)                     # this works
