import nimdata
import nimdata/utils
import strutils

UnitTestSuite("JoinThetaDataFrame"):
  test "join":

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
    check joined.collect() == @[
      (name: "A", age: 99, height: 1.80),
      (name: "A", age: 99, height: 1.50),
    ]
    check joined.collect() == @[
      (name: "A", age: 99, height: 1.80),
      (name: "A", age: 99, height: 1.50),
    ]

UnitTestSuite("JoinEquiDataFrame"):
  test "join":

    let dfA = DF.fromSeq(@[
      (name: "A", age: 99)
    ])

    let dfB = DF.fromSeq(@[
      (name: "A", height: 1.80),
      (name: "A", height: 1.50),
      (name: "B", height: 1.50),
    ])

    let joined = joinEqui(
      dfA,
      dfB,
      a => a.name,
      b => b.name,
      (a, b) => mergeTuple(a, b, ["name"])
    )

    check joined.collect() == @[
      (name: "A", age: 99, height: 1.80),
      (name: "A", age: 99, height: 1.50),
    ]
    check joined.collect() == @[
      (name: "A", age: 99, height: 1.80),
      (name: "A", age: 99, height: 1.50),
    ]

  test "join macro":
    let dfA = DF.fromSeq(@[
      (name: "A", age: 99)
    ])

    let dfB = DF.fromSeq(@[
      (name: "A", height: 1.80),
      (name: "A", height: 1.50),
      (name: "B", height: 1.50),
    ])

    let joined = join(dfA, dfB, [name])

    check joined.collect() == @[
      (name: "A", age: 99, height: 1.80),
      (name: "A", age: 99, height: 1.50),
    ]

  test "join macro":
    let dfA = DF.fromSeq(@[
      (name: "A", age: 99, id: 1)
    ])

    let dfB = DF.fromSeq(@[
      (name: "A", height: 1.80, id: 1),
      (name: "A", height: 1.50, id: 2),
      (name: "B", height: 1.50, id: 3),
    ])

    let joined = join(dfA, dfB, [id, name])

    check joined.collect() == @[
      (id: 1, name: "A", age: 99, height: 1.80),
    ]