import strutils
import nimdata
import nimdata/utils

proc example01*() =
  let input = @[
      "Jon;22",
      "Bart;33",
      "Bob;49",
      "Jack;12",
      "Moe;58",
  ]
  const schema = [
    strCol("name"),
    intCol("age")
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
  dfRawText.take(5).show()

  # Now let's parse the CSV into typesafe tuple objects using `map`. Since
  # our data set is small and we want to perform multiple operations on it,
  # it makes sense to load the parsing result into memory by using `cache`.
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
             .map(record => record.projectAway(index))
             .cache()

  # Check number of rows again
  echo df.count()

  # Show first 5 records; records are now a type safe tuple object.
  df.take(5).show()

  # Note that it is always possible to write the entire pipeline as follows
  # (will read the file from scratch):
  DF.fromFile("examples/Bundesliga.csv")
    .map(schemaParser(schema, ','))
    .map(record => record.projectAway(index))
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

  # Sort example: Sort by number of away goals:
  df.sort(record => record.awayGoals, SortOrder.Descending).take(5).show()

  # Check the total number of teams:
  echo df.map(record => record.homeTeam).unique().count()

  # Using unique with multiple dimensions:
  df.map(record => record.projectTo(homeTeam, awayTeam)).unique().take(5).show()

  # Let's find the most frequent results by using `valueCounts`
  df.map(record => record.projectTo(homeGoals, awayGoals))
    .valueCounts()
    .sort(x => x.count, SortOrder.Descending)
    .map(x => (
      homeGoals: x.key.homeGoals,
      awayGoals: x.key.awayGoals,
      count: x.count
    ))
    .take(5)
    .show()

  # lets look at the 15 most successful home teams
  df.filter(record => record.homeGoals > record.awayGoals)
    .map(record => record.projectTo(homeTeam))
    .valueCounts()
    .sort(x => x.count, SortOrder.Descending)
    .map(x => (
      homeTeam: x.key.homeTeam,
      count: x.count
    ))
    .take(15)
    .barPlot(x = homeTeam, y = count)
    .show()

  # a scatter plots of homeGoals / awayGoals
  df.scatterPlot(x = homeGoals, y = awayGoals)
    .show()

  # and a heatmap of these
  df.map(record => record.projectTo(homeGoals, awayGoals))
    .valueCounts()
    .sort(x => x.count, SortOrder.Descending)
    .map(x => (
      homeGoals: x.key.homeGoals,
      awayGoals: x.key.awayGoals,
      count: x.count
    ))
    .heatmap(x = homeGoals, y = awayGoals, z = count)
    .show()

  # alternatively a coloread scatter plot of the same, with the
  # number of occurences as the color of the marker
  df.map(record => record.projectTo(homeGoals, awayGoals))
    .valueCounts()
    .sort(x => x.count, SortOrder.Descending)
    .map(x => (
      homeGoals: x.key.homeGoals,
      awayGoals: x.key.awayGoals,
      count: x.count
    ))
    .scatterColor(x = homeGoals, y = awayGoals, z = count)
    .markerSize(20)
    .show()

  # To open data frame in browser:
  # df.filter(x => x.year == 2000).openInBrowser()

when isMainModule:
  # example01()
  example02()
