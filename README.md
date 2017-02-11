# NimData

Early prototype of a DataFrame API in Nim.

Allows to write code like:

```nimrod
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
           .map(schemaParser(schema))
           .filter(person => person.age > 10)
           .filter(person => person.name.startsWith("B"))
           .sample(probability=1.0)
           # up to this point nothing has happened, all transformations are lazy.
           .cache()
           # this call performs all transformations and caches the result in memory.

echo df.collect()
# => @[(name: Bart, age: 33), (name: Bob, age: 49)]
echo df.map(x => x.age).collect()
# => @[33, 49]
```

## Documentation

See [module docs](https://bluenote10.github.io/NimData/nimdata.html).
