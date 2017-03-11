# Simple demostration of join

import nimdata

let dfA = DF.fromSeq(@[
  (name: "A", age: 99)
])

let dfB = DF.fromSeq(@[
  (name: "A", height: 1.80),
  (name: "A", height: 1.50),
  (name: "B", height: 1.50),
])

let joined = joinTheta(
  dfA,
  dfB,
  (a, b) => a.name == b.name,
  (a, b) => mergeTuple(a, b, ["name"])
)

joined.show()

# Most importantly: With a working nimsuggest setup the
# inferred result type can even be seen from auto-completion:
echo joined.collect()[0].name