import nimdata
import nimdata/utils
import strutils

UnitTestSuite("JoinDataFrame"):
  test "join":

    let dfA = DF.fromSeq(@[
      (name: "A", age: 99)
    ])

    let dfB = DF.fromSeq(@[
      (name: "A", height: 1.80),
      (name: "A", height: 1.50),
      (name: "B", height: 1.50),
    ])

    let joined = join(
      dfA,
      dfB,
      (a, b) => a.name == b.name,
      (a, b) => mergeTuple(a, b, ["name"])
    )
    check joined.collect() == @[
      (name: "A", age: 99, height: 1.80),
      (name: "A", age: 99, height: 1.50),
    ]

