
import future
import strutils

import nimdata


proc example01() =
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


proc example02() =
  let df = DF.fromFile("table.csv")
  echo df.count()
  echo df.collect()


example01()
example02()

