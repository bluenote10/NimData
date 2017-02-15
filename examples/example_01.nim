
import future
import strutils
import nimdata
import nimdata_utils


proc example01*() =
  let input = @[
      "Jon;22",
      "Bart;33",
      "Bob;49",
      "Jack;12",
      "Moe;58",
  ]
  const schema = [
    col(StrCol, "name"),
    col(IntCol, "age")
  ]

  let df = DF.fromSeq(input)
             .map(schemaParser(schema, ';'))
             .filter(person => person.age > 10)
             .filter(person => person.name.startsWith("B"))
             .sample(probability = 1.0)
             # up to this point nothing has happened, transformations are lazy.
             .cache()
             # this call performs all transformations and caches the result in memory.

  # echo df.count() # causes runtime error :(
  echo df.collect()
  echo df.map(x => x.age).collect()

  echo df.map(x => x.age).mean()
  echo df.map(x => x.age).min()
  echo df.map(x => x.age).max()

  df.toHtml("table.html")
  df.toCsv("table.csv")
  df.openInBrowser()


proc example02*() =

  # Load a CSV
  let dfRawText = DF.fromFile("examples/Bundesliga.csv")

  # Check number of rows
  echo dfRawText.count()

  # Show first 5 rows:
  dfRawText.take(5).forEach(echoGeneric)

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

  # Show first 5 records; records are now a type safe tuple object.
  df.take(5).show()

  # Note that it is always possible to write the entire pipeline as follows
  # (will read the file from scratch):
  DF.fromFile("examples/Bundesliga.csv")
    .map(schemaParser(schema, ','))
    .take(5)
    .show()

  # Data can be filtered by using `filter`, which can be used to get games
  # of a certain team...
  df.filter(record =>
      record.homeTeam.contains("Freiburg") or
      record.awayTeam.contains("Freiburg")
    )
    .take(5)
    .show()

  # ... or e.g. games with many home goals:
  df.filter(record => record.homeGoals >= 10)
    .show()

  # A DataFrame[T] can be converted easily into seq[T] (Nim's native dynamic
  # arrays) by using `collect`.
  let manyGoalsVector = df
    .map(record => record.homeGoals)
    .filter(goals => goals >= 10)
    .collect()
  echo manyGoalsVector

  # A DataFrame of a numerical type allows to use functions like min/max/mean.
  # This allows to get things like:
  echo "Min date: ", df.map(record => record.year).min()
  echo "Max date: ", df.map(record => record.year).max()
  echo "Average home goals: ", df.map(record => record.homeGoals).mean()
  echo "Average away goals: ", df.map(record => record.awayGoals).mean()

  # Let's find the highest defeat
  let maxDiff = df.map(record => (record.homeGoals - record.awayGoals).abs).max()
  df.filter(record => (record.homeGoals - record.awayGoals) == maxDiff)
    .show()


when isMainModule:
  # example01()
  example02()
