import nimdata
import nimdata/utils
import strutils

UnitTestSuite("JoinDataFrame"):
  test "Construction":

    let dfA = DF.fromSeq(@[
      (name: "A", age: 99)
    ])

    let dfB = DF.fromSeq(@[
      (name: "A", height: 1.80)
    ])

    let joined = dfA.join(dfB, @["name"])

