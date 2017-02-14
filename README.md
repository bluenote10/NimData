# NimData  [![Build Status](https://travis-ci.org/bluenote10/NimData.svg?branch=master)](https://travis-ci.org/bluenote10/NimData) [![license](https://img.shields.io/github/license/mashape/apistatus.svg)](LICENSE) <a href="https://github.com/yglukhov/nimble-tag"><img src="https://raw.githubusercontent.com/yglukhov/nimble-tag/master/nimble.png" height="23" ></a>

DataFrame API in Nim, enabling fast out-of-core data processing.

NimData is inspired by frameworks like Pandas/Spark/Flink/Thrill,
and sits between the Pandas and the Spark/Flink/Thrill side.
Similar to Pandas, NimData is currently non-distributed,
but shares the type-safe, lazy API of Spark/Flink/Thrill.
Thanks to Nim, it enables elegant out-of-core processing at native speed.

## Documentation

NimData's core data type is a generic `DataFrame[T]`. The methods
of a data frame can be categorized into generalizations
of the Map/Reduce concept:

- **Transformations**: Operations like `map` or `filter` transform one data
frame into another. Transformations are lazy and can be chained. They will only
be executed once an action is called.
- **Actions**: Operations like `count`, `min`, `max`, `sum`, `reduce`, `fold`, or `collect`
perform an aggregation of a data frame, and trigger the processing pipeline.

For a complete reference of the supported operations in NimData refer to the
[module docs](https://bluenote10.github.io/NimData/nimdata.html).

The following tutorial will give a brief introduction of the main
functionality based on [this](examples/Bundesliga.csv) German soccer data set.

### Reading raw text data

To create a data frame which simply iterates over the raw text content
of a file, we can use `DF.fromFile`:

```nimrod
let dfRawText = DF.fromFile("examples/Bundesliga.csv")
```

The operation is lazy, so nothing happens so far.
The type of the `dfRawText` is a plain `DataFrame[string]`.
We can still perform some initial checks on it:

```nimrod
echo dfRawText.count()
# => 14018
```

The `count()` method is an action, which triggers the line-by-line reading of the
file, returning the number of rows. We can re-use `dfRawText` with different
transformations/actions. The following would filter the file to the first
5 rows and perform a `forEach` action to print the records.


```nimrod
dfRawText.take(5).forEach(echoGeneric)
# =>
# "1","Werder Bremen","Borussia Dortmund",3,2,1,1963,1963-08-24 09:30:00
# "2","Hertha BSC Berlin","1. FC Nuernberg",1,1,1,1963,1963-08-24 09:30:00
# "3","Preussen Muenster","Hamburger SV",1,1,1,1963,1963-08-24 09:30:00
# "4","Eintracht Frankfurt","1. FC Kaiserslautern",1,1,1,1963,1963-08-24 09:30:00
# "5","Karlsruher SC","Meidericher SV",1,4,1,1963,1963-08-24 09:30:00
```

Each action call results in the file being read from scratch.

### Type-safe schema parsing

Now let's parse the CSV into type-safe tuple objects using `map`.
The price for achieving compile time safety is that the schema
has to be specified once for the compiler.
Fortunately, Nim's meta programming capabilities make this very
straightforward. The following example uses the `schemaParser`
macro. This macro automatically generates a parsing function,
which takes a `string` as input and returns a type-safe tuple
with fields corresponding to the `schema` definition.

Since our data set is small and we want to perform multiple operations on it,
it makes sense to persist the parsing result into memory.
This can be done by using `cache()` method.
As a result, all operations performed on `df` will not have to re-read
the file, but read the already parsed data from memory.
_Spark users note_: In contrast to Spark, `cache()` is currently implemented
as an action.

```nimrod
const schema = [
  col(StrCol, "index"),
  col(StrCol, "homeTeam"),
  col(StrCol, "awayTeam"),
  col(IntCol, "homeGoals"),
  col(IntCol, "awayGoals"),
  col(IntCol, "round"),
  col(IntCol, "year"),
  col(StrCol, "date") # proper timestamp parsing coming soon
]
let df = dfRawText.map(schemaParser(schema, ','))
                  .cache()
```

We can perform the same checks as before, but this time the data frame
contains the parsed tuples:

```nimrod
echo df.count()
# => 14018

df.take(5).forEach(echoGeneric)
# =>
# (index: "1", homeTeam: "Werder Bremen", awayTeam: "Borussia Dortmund", homeGoals: 3, awayGoals: 2, round: 1, year: 1963, date: 1963-08-24 09:30:00)
# (index: "2", homeTeam: "Hertha BSC Berlin", awayTeam: "1. FC Nuernberg", homeGoals: 1, awayGoals: 1, round: 1, year: 1963, date: 1963-08-24 09:30:00)
# (index: "3", homeTeam: "Preussen Muenster", awayTeam: "Hamburger SV", homeGoals: 1, awayGoals: 1, round: 1, year: 1963, date: 1963-08-24 09:30:00)
# (index: "4", homeTeam: "Eintracht Frankfurt", awayTeam: "1. FC Kaiserslautern", homeGoals: 1, awayGoals: 1, round: 1, year: 1963, date: 1963-08-24 09:30:00)
# (index: "5", homeTeam: "Karlsruher SC", awayTeam: "Meidericher SV", homeGoals: 1, awayGoals: 4, round: 1, year: 1963, date: 1963-08-24 09:30:00)
```

Note that instead of starting the pipeline from `dfRawText` and using
caching, we could always write the pipeline from scratch:

```nimrod
DF.fromFile("examples/Bundesliga.csv")
  .map(schemaParser(schema, ','))
  .take(5)
  .forEach(echoGeneric)
```

### Simple transformations: filter

Data can be filtered by using `filter`. For instance, we can filter the data to get games
of a certain team only:

```nimrod
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
```

Or search for games with many home goals:

```nimrod
df.filter(record => record.homeGoals >= 10)
  .forEach(echoGeneric)
# =>
# (index: "944", homeTeam: "Borussia Moenchengladbach", awayTeam: "Schalke 04", homeGoals: 11, awayGoals: 0, round: 18, year: 1966, date: 1967-01-07 08:30:00)
# (index: "1198", homeTeam: "Borussia Moenchengladbach", awayTeam: "Borussia Neunkirchen", homeGoals: 10, awayGoals: 0, round: 12, year: 1967, date: 1967-11-04 08:30:00)
# (index: "2456", homeTeam: "Bayern Muenchen", awayTeam: "Borussia Dortmund", homeGoals: 11, awayGoals: 1, round: 16, year: 1971, date: 1971-11-27 08:30:00)
# (index: "4457", homeTeam: "Borussia Moenchengladbach", awayTeam: "Borussia Dortmund", homeGoals: 12, awayGoals: 0, round: 34, year: 1977, date: 1978-04-29 08:30:00)
# (index: "5788", homeTeam: "Borussia Dortmund", awayTeam: "Arminia Bielefeld", homeGoals: 11, awayGoals: 1, round: 12, year: 1982, date: 1982-11-06 08:30:00)
# (index: "6364", homeTeam: "Borussia Moenchengladbach", awayTeam: "Eintracht Braunschweig", homeGoals: 10, awayGoals: 0, round: 8, year: 1984, date: 1984-10-11 14:00:00)
```

Note that we can now fully benefit from type-safety:
The compiler knows the exact fields and types of a record.
No dynamic field lookup and/or type casting is required.
Assumptions about the data structure are moved to the earliest
possible step in the pipeline, allowing to fail early if they
are wrong. After transitioning into the type-safe domain, the
compiler helps to verify the correctness of even long processing
pipelines, reducing the risk of runtime errors.

Other filter-like transformation are:

- `take`, which takes the first N records as already seen.
- `drop`, which discard the first N records.
- `filterWithIndex`. which allows to define a filter function that take both the index and the elements as input.

### Collecting data

A `DataFrame[T]` can be converted easily into a `seq[T]` (Nim's native dynamic
arrays) by using `collect`:

```nimrod
echo df.map(record => record.homeGoals)
       .filter(goals => goals >= 10)
       .collect()
# => @[11, 10, 11, 12, 11, 10]
```

### Numerical aggregation

A DataFrame of a numerical type allows to use functions like `min`/`max`/`mean`.
This allows to get things like:

```nimrod
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

## Installation (for users new to Nim)

NimData requires to have Nim installed. On systems where a C compiler and git is available,
the best method is to [compile Nim](https://github.com/nim-lang/nim#compiling) from
the GitHub sources. Modern versions of Nim include Nimble (Nim's package manager),
so installing NimData becomes:

    $ nimble install NimData

This will download the NimData source from GitHub and put it in `~/.nimble/pkgs`.
A minimal NimData program would look like:

```nim
import nimdata

echo DF.fromRange(0, 10).collect()
```

To compile and run the program use `nim -r c test.nim` (`c` for compile, and `-r` to run directly after compilation).

## Next steps

- More transformation/actions (flatMap, groupBy, join, sort, union, window)
- More data formats
- Plotting (maybe in the form of Bokeh bindings)
- REPL or Jupyter kernel

## License

This project is licensed under the terms of the MIT license.
