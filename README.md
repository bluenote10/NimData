# NimData

Prototype of a DataFrame API in Nim, enabling fast out-of-core data processing.

NimData is inspired by frameworks like Pandas/Spark/Flink/Thrill,
and sits between the Pandas and the Spark/Flink/Thrill side.
Similar to Pandas, NimData is currently non-distributed,
but shares the type-safe, lazy API of Spark/Flink/Thrill.
Thanks to Nim, it enables elegant out-of-core processing at native speed.

Allows to write code like (example based on this [German soccer data set](examples/Bundesliga.csv)):

```nimrod
# Load a raw CSV text file. No parsing is done here yet, this is basically
# just an iterator over rows of type string:
let dfRawText = DF.fromFile("examples/Bundesliga.csv")

# Check number of rows:
echo dfRawText.count()
# => 14018

# Show first 5 rows:
dfRawText.take(5).forEach(echoGeneric)
# =>
# "1","Werder Bremen","Borussia Dortmund",3,2,1,1963,1963-08-24 09:30:00
# "2","Hertha BSC Berlin","1. FC Nuernberg",1,1,1,1963,1963-08-24 09:30:00
# "3","Preussen Muenster","Hamburger SV",1,1,1,1963,1963-08-24 09:30:00
# "4","Eintracht Frankfurt","1. FC Kaiserslautern",1,1,1,1963,1963-08-24 09:30:00
# "5","Karlsruher SC","Meidericher SV",1,4,1,1963,1963-08-24 09:30:00

# Now let's parse the CSV into typesafe tuple objects using `map`. Since
# our data set is small and we want to perform multiple operations on it,
# it makes sense to load the parsing result into memory by using `cache`.
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
let df = DF.fromFile("examples/Bundesliga.csv")
           .map(schemaParser(schema, ','))
           .cache()

# Check number of rows again
echo df.count()
# => 14018

# Show first 5 records; records are now a type safe tuple object.
df.take(5).forEach(echoGeneric)
# =>
# (index: "1", homeTeam: "Werder Bremen", awayTeam: "Borussia Dortmund", homeGoals: 3, awayGoals: 2, round: 1, year: 1963, date: 1963-08-24 09:30:00)
# (index: "2", homeTeam: "Hertha BSC Berlin", awayTeam: "1. FC Nuernberg", homeGoals: 1, awayGoals: 1, round: 1, year: 1963, date: 1963-08-24 09:30:00)
# (index: "3", homeTeam: "Preussen Muenster", awayTeam: "Hamburger SV", homeGoals: 1, awayGoals: 1, round: 1, year: 1963, date: 1963-08-24 09:30:00)
# (index: "4", homeTeam: "Eintracht Frankfurt", awayTeam: "1. FC Kaiserslautern", homeGoals: 1, awayGoals: 1, round: 1, year: 1963, date: 1963-08-24 09:30:00)
# (index: "5", homeTeam: "Karlsruher SC", awayTeam: "Meidericher SV", homeGoals: 1, awayGoals: 4, round: 1, year: 1963, date: 1963-08-24 09:30:00)

# Note that it is always possible to write the entire pipeline as follows
# (will read the file from scratch):
DF.fromFile("examples/Bundesliga.csv")
  .map(schemaParser(schema, ','))
  .take(5)
  .forEach(echoGeneric)

# Data can be filtered by using `filter`, which can be used to get games
# of a certain team...
df.filter(record =>
    record.homeTeam.contains("Freiburg") or
    record.awayTeam.contains("Freiburg")
  )
  .take(5)
  .forEach(echoGeneric)
# =>
# (index: "9128", homeTeam: "Bayern Muenchen", awayTeam: "SC Freiburg", homeGoals: 3, awayGoals: 1, round: 1, year: 1993, date: 1993-08-07 08:30:00)
# (index: "9135", homeTeam: "SC Freiburg", awayTeam: "Wattenscheid 09", homeGoals: 4, awayGoals: 1, round: 2, year: 1993, date: 1993-08-14 08:30:00)
# (index: "9147", homeTeam: "Borussia Dortmund", awayTeam: "SC Freiburg", homeGoals: 3, awayGoals: 2, round: 3, year: 1993, date: 1993-08-21 08:30:00)
# (index: "9150", homeTeam: "SC Freiburg", awayTeam: "Hamburger SV", homeGoals: 0, awayGoals: 1, round: 4, year: 1993, date: 1993-08-27 12:30:00)
# (index: "9164", homeTeam: "1. FC Koeln", awayTeam: "SC Freiburg", homeGoals: 2, awayGoals: 0, round: 5, year: 1993, date: 1993-09-01 12:30:00)

# ... or e.g. games with many home goals:
df.filter(record => record.homeGoals >= 10)
  .forEach(echoGeneric)
# =>
# (index: "944", homeTeam: "Borussia Moenchengladbach", awayTeam: "Schalke 04", homeGoals: 11, awayGoals: 0, round: 18, year: 1966, date: 1967-01-07 08:30:00)
# (index: "1198", homeTeam: "Borussia Moenchengladbach", awayTeam: "Borussia Neunkirchen", homeGoals: 10, awayGoals: 0, round: 12, year: 1967, date: 1967-11-04 08:30:00)
# (index: "2456", homeTeam: "Bayern Muenchen", awayTeam: "Borussia Dortmund", homeGoals: 11, awayGoals: 1, round: 16, year: 1971, date: 1971-11-27 08:30:00)
# (index: "4457", homeTeam: "Borussia Moenchengladbach", awayTeam: "Borussia Dortmund", homeGoals: 12, awayGoals: 0, round: 34, year: 1977, date: 1978-04-29 08:30:00)
# (index: "5788", homeTeam: "Borussia Dortmund", awayTeam: "Arminia Bielefeld", homeGoals: 11, awayGoals: 1, round: 12, year: 1982, date: 1982-11-06 08:30:00)
# (index: "6364", homeTeam: "Borussia Moenchengladbach", awayTeam: "Eintracht Braunschweig", homeGoals: 10, awayGoals: 0, round: 8, year: 1984, date: 1984-10-11 14:00:00)

# A DataFrame[T] can be converted easily into seq[T] (Nim's native dynamic
# arrays) by using `collect`.
let manyGoalsVector = df
  .map(record => record.homeGoals)
  .filter(goals => goals >= 10)
  .collect()
echo manyGoalsVector
# => @[11, 10, 11, 12, 11, 10]

# A DataFrame of a numerical type allows to use functions like min/max/mean.
# This allows to get things like:
echo "Min date: ", df.map(record => record.year).min()
echo "Max date: ", df.map(record => record.year).max()
echo "Average home goals: ", df.map(record => record.homeGoals).mean()
echo "Average away goals: ", df.map(record => record.awayGoals).mean()
# =>
# Min date: 1963
# Max date: 2008
# Average home goals: 1.898130974461407
# Average away goals: 1.190754743900699

# Let's find the highest defeat
let maxDiff = df.map(record => (record.homeGoals - record.awayGoals).abs).max()
df.filter(record => (record.homeGoals - record.awayGoals) == maxDiff)
  .forEach(echoGeneric)
# =>
# (index: "4457", homeTeam: "Borussia Moenchengladbach", awayTeam: "Borussia Dortmund", homeGoals: 12, awayGoals: 0, round: 34, year: 1977, date: 1978-04-29 08:30:00)
```

## Documentation

See [module docs](https://bluenote10.github.io/NimData/nimdata.html).

## Next steps

- More transformation/actions (flatMap, groupBy, join, sort, union, window)
- More data formats
- Plotting (maybe in the form of Bokeh bindings)
- REPL or Jupyter kernel

## License

This project is licensed under the terms of the MIT license.
