import nimdata
import nimdata/utils
import strutils

UnitTestSuite("JoinDataFrame"):
  test "Construction":

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
      (a, b) => joinTuple(a, b, ["name"])
    )
    joined.show()

