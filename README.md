# NimData  [![Build Status](https://travis-ci.org/bluenote10/NimData.svg?branch=master)](https://travis-ci.org/bluenote10/NimData) [![license](https://img.shields.io/github/license/mashape/apistatus.svg)](LICENSE) <a href="https://github.com/yglukhov/nimble-tag"><img src="https://raw.githubusercontent.com/yglukhov/nimble-tag/master/nimble.png" height="23" ></a>

DataFrame API written in Nim, enabling [fast](#benchmarks) out-of-core data processing.

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
- **Actions**: Operations like `count`, `min`, `max`, `sum`, `reduce`, `fold`, `collect`, or `show`
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
5 rows and perform a `show` action to print the records.


```nimrod
dfRawText.take(5).show()
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
  strCol("index"),
  strCol("homeTeam"),
  strCol("awayTeam"),
  intCol("homeGoals"),
  intCol("awayGoals"),
  intCol("round"),
  intCol("year"),
  dateCol("date", format="yyyy-MM-dd hh:mm:ss")
]
let df = dfRawText.map(schemaParser(schema, ','))
                  .cache()
```

We can perform the same checks as before, but this time the data frame
contains the parsed tuples:

```nimrod
echo df.count()
# => 14018

df.take(5).show()
# =>
# +------------+------------+------------+------------+------------+------------+------------+------------+
# | index      | homeTeam   | awayTeam   |  homeGoals |  awayGoals |      round |       year | date       |
# +------------+------------+------------+------------+------------+------------+------------+------------+
# | "1"        | "Werder B… | "Borussia… |          3 |          2 |          1 |       1963 | 1963-08-2… |
# | "2"        | "Hertha B… | "1. FC Nu… |          1 |          1 |          1 |       1963 | 1963-08-2… |
# | "3"        | "Preussen… | "Hamburge… |          1 |          1 |          1 |       1963 | 1963-08-2… |
# | "4"        | "Eintrach… | "1. FC Ka… |          1 |          1 |          1 |       1963 | 1963-08-2… |
# | "5"        | "Karlsruh… | "Meideric… |          1 |          4 |          1 |       1963 | 1963-08-2… |
# +------------+------------+------------+------------+------------+------------+------------+------------+
```

Note that instead of starting the pipeline from `dfRawText` and using
caching, we could always write the pipeline from scratch:

```nimrod
DF.fromFile("examples/Bundesliga.csv")
  .map(schemaParser(schema, ','))
  .take(5)
  .show()
```

### Filter

Data can be filtered by using `filter`. For instance, we can filter the data to get games
of a certain team only:

```nimrod
df.filter(record =>
    record.homeTeam.contains("Freiburg") or
    record.awayTeam.contains("Freiburg")
  )
  .take(5)
  .show()
# =>
# +------------+------------+------------+------------+------------+------------+------------+------------+
# | index      | homeTeam   | awayTeam   |  homeGoals |  awayGoals |      round |       year | date       |
# +------------+------------+------------+------------+------------+------------+------------+------------+
# | "9128"     | "Bayern M… | "SC Freib… |          3 |          1 |          1 |       1993 | 1993-08-0… |
# | "9135"     | "SC Freib… | "Wattensc… |          4 |          1 |          2 |       1993 | 1993-08-1… |
# | "9147"     | "Borussia… | "SC Freib… |          3 |          2 |          3 |       1993 | 1993-08-2… |
# | "9150"     | "SC Freib… | "Hamburge… |          0 |          1 |          4 |       1993 | 1993-08-2… |
# | "9164"     | "1. FC Ko… | "SC Freib… |          2 |          0 |          5 |       1993 | 1993-09-0… |
# +------------+------------+------------+------------+------------+------------+------------+------------+
```

Or search for games with many home goals:

```nimrod
df.filter(record => record.homeGoals >= 10)
  .show()
# =>
# +------------+------------+------------+------------+------------+------------+------------+------------+
# | index      | homeTeam   | awayTeam   |  homeGoals |  awayGoals |      round |       year | date       |
# +------------+------------+------------+------------+------------+------------+------------+------------+
# | "944"      | "Borussia… | "Schalke … |         11 |          0 |         18 |       1966 | 1967-01-0… |
# | "1198"     | "Borussia… | "Borussia… |         10 |          0 |         12 |       1967 | 1967-11-0… |
# | "2456"     | "Bayern M… | "Borussia… |         11 |          1 |         16 |       1971 | 1971-11-2… |
# | "4457"     | "Borussia… | "Borussia… |         12 |          0 |         34 |       1977 | 1978-04-2… |
# | "5788"     | "Borussia… | "Arminia … |         11 |          1 |         12 |       1982 | 1982-11-0… |
# | "6364"     | "Borussia… | "Eintrach… |         10 |          0 |          8 |       1984 | 1984-10-1… |
# +------------+------------+------------+------------+------------+------------+------------+------------+
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
- `filterWithIndex`, which allows to define a filter function that take both the index and the elements as input.

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
  .show()
# =>
# +------------+------------+------------+------------+------------+------------+------------+------------+
# | index      | homeTeam   | awayTeam   |  homeGoals |  awayGoals |      round |       year | date       |
# +------------+------------+------------+------------+------------+------------+------------+------------+
# | "4457"     | "Borussia… | "Borussia… |         12 |          0 |         34 |       1977 | 1978-04-2… |
# +------------+------------+------------+------------+------------+------------+------------+------------+
```

### Sorting

A data frame can be transformed into a sorted data frame by the `sort()` method.
Without specifying any arguments, the operation would sort using default
comparison over all columns. By specifying a key function and the sort order,
we can for instance rank the games by the number of away goals:

```nimrod
df.sort(record => record.awayGoals, SortOrder.Descending)
  .take(5)
  .show()
# =>
# +------------+------------+------------+------------+------------+------------+------------+------------+
# | index      | homeTeam   | awayTeam   |  homeGoals |  awayGoals |      round |       year | date       |
# +------------+------------+------------+------------+------------+------------+------------+------------+
# | "720"      | "Tasmania… | "Meideric… |          0 |          9 |         27 |       1965 | 1966-03-2… |
# | "740"      | "Borussia… | "TSV 1860… |          1 |          9 |         29 |       1965 | 1966-04-1… |
# | "11181"    | "SSV Ulm"  | "Bayer Le… |          1 |          9 |         25 |       1999 | 2000-03-1… |
# | "4128"     | "Rot-Weis… | "Eintrach… |          1 |          8 |         32 |       1976 | 1977-05-0… |
# | "10735"    | "Borussia… | "Bayer Le… |          2 |          8 |         10 |       1998 | 1998-10-3… |
# +------------+------------+------------+------------+------------+------------+------------+------------+
```

### Unique values

The `DataFrame[T].unique()` transformation filters a data frame to unique elements.
This can be used for instance to find the number of teams that appear in the data:

```nimrod
echo df.map(record => record.homeTeam).unique().count()
# => 52
```

_Pandas user note_: In contrast to Pandas, there is no differentiation between
a one-dimensional series and multi-dimensional data frame (`unique` vs `drop_duplicates`).
`unique` works the same in for any hashable type `T`, e.g., we might as well get
a data frame of unique pairs:

```nimrod
df.map(record => (record.homeTeam, record.awayTeam))
  .unique()
  .take(5)
  .show()
# =>
# +------------+------------+
# | Field0     | Field1     |
# +------------+------------+
# | "Werder B… | "Borussia… |
# | "Hertha B… | "1. FC Nu… |
# | "Preussen… | "Hamburge… |
# | "Eintrach… | "1. FC Ka… |
# | "Karlsruh… | "Meideric… |
# +------------+------------+
```

### Value counts

The `DataFrame[T].valueCounts()` transformation extends the functionality of
`unique()` by returning the unique values and their respective counts.
The type of the transformed data frame is a tuple of `(key: T, count: int)`,
where `T` is the original type.

In our example, we can use `valueCounts()` for instance to find the most
frequent results in German soccer:

```nimrod
df.map(record => (
    homeGoals: record.homeGoals,
    awayGoals: record.awayGoals
  ))
  .valueCounts()
  .sort(x => x.count, SortOrder.Descending)
  .map(x => (
    homeGoals: x.key.homeGoals,
    awayGoals: x.key.awayGoals,
    count: x.count
  ))
  .take(5)
  .show()
# =>
# +------------+------------+------------+
# |  homeGoals |  awayGoals |      count |
# +------------+------------+------------+
# |          1 |          1 |       1632 |
# |          2 |          1 |       1203 |
# |          1 |          0 |       1109 |
# |          2 |          0 |       1092 |
# |          0 |          0 |        914 |
# +------------+------------+------------+
```

This transformation first projects the data onto a named tuple of
`(homeGoals, awayGoals)`. After applying `valueCounts()` the data
frame is sorted according to the counts. The final `map()` function
is purely for cosmetics of the resulting table, projecting the nested
`(key: (homeGaols: int, awayGoals: int), counts: int)` tuple back
to a flat result.

### Data frame viewer

Data frames can be opened and inspected in the browser by using `df.openInBrowser()`,
which offers a simple Javascript based data browser:

![Viewer example](docs/viewer_example.png)

Note that the viewer uses static HTML, so it should only be applied to small
or heavily filtered data frames.


## Installation (for users new to Nim)

NimData requires to have Nim installed. On systems where a C compiler and git is available,
the best method is to compile Nim from the GitHub sources. Modern versions of Nim include
Nimble (Nim's package manager), and [building](https://github.com/nim-lang/nim#compiling)
them both would look like:

```bash
# clone Nim
git clone https://github.com/nim-lang/Nim.git
cd Nim

# build the C sources
git clone --depth 1 https://github.com/nim-lang/csources.git
cd csources
sh build.sh
cd ../

# build Nim & Nimble
bin/nim c koch
./koch boot -d:release
./koch nimble

# add ./bin to path
export PATH=$PATH:`readlink -f ./bin`
```

With Nim and Nimble installed, installing NimData becomes:

    $ nimble install NimData

This will download the NimData source from GitHub and put it in `~/.nimble/pkgs`.
A minimal NimData program would look like:

```nim
import nimdata

echo DF.fromRange(0, 10).collect()
```

To compile and run the program use `nim -r c test.nim` (`c` for compile, and `-r` to run directly after compilation).

## Benchmarks

More meaningful benchmarks are still on the todo list. This just shows a
few first results. The benchmarks will be split into small (data
which fits into memory so we can compare against Pandas or R easily) and
big (where we can only compare against out-of-core frameworks).

All implementations are available in the [benchmarks](benchmarks) folder.

### Basic operations (small data)

The test data set is 1 million rows CSV with two int and two float columns.
The test tasks are:

- Parse/Count: Just the most basic operations -- iterating the file, applying
parsing, and return a count.
- Column Averages: Same steps, plus an additional computation of all 4 column means.

The results are average runtime in seconds of three runs:

| Task                    |          NimData |           Pandas |  Spark (4 cores) |   Dask (4 cores) |
|:------------------------|-----------------:|-----------------:|-----------------:|-----------------:|
| Parse/Count             |            0.165 |            0.321 |            1.606 |            0.182 |
| Column Averages         |            0.259 |            0.340 |            1.179 |            0.622 |

Note that Spark internally caches the file over the three runs, so the first iteration
is much slower (with > 3 sec) while it reaches run times of 0.6 sec in the last iterations
(obviously the data is too small to justify the overhead anyway).


## Next steps

- More transformations:
  - [x] map
  - [x] filter
  - [x] flatMap
  - [x] sort
  - [x] unique
  - [x] valueCounts
  - [x] groupBy (reduce)
  - [ ] groupBy (transform)
  - [x] join (inner)
  - [ ] join (outer)
  - [ ] concat/union
  - [ ] window
- More actions:
  - [x] numerical aggergations (count, min, max, sum, mean)
  - [x] collect
  - [x] show
  - [x] openInBrowser
- More data formats/sources
  - [x] csv
  - [x] gzipped csv
  - [ ] parquet
  - [ ] S3
- REPL or Jupyter kernel?
- Plotting (maybe in the form of Bokeh bindings)?

## License

This project is licensed under the terms of the MIT license.
