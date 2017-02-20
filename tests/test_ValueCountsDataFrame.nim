import nimdata
import nimdata/utils

UnitTestSuite("ValueCountsDataFrame"):
  test "valueCounts":
    check DF.fromSeq(@[1, 1, 2]).valueCounts().sort(x => x.count).collect() == @[
      (key: 2, count: 1),
      (key: 1, count: 2),
    ]
    check DF.fromSeq(@[1, 2, 1]).valueCounts().sort(x => x.count).collect() == @[
      (key: 2, count: 1),
      (key: 1, count: 2),
    ]
    check DF.fromSeq(@[2, 1, 1]).valueCounts().sort(x => x.count).collect() == @[
      (key: 2, count: 1),
      (key: 1, count: 2),
    ]

    let tupleDataDuplicates = @[
      (id: 1, name: "A"),
      (id: 1, name: "A"),
      (id: 2, name: "X"),
      (id: 1, name: "A"),
      (id: 2, name: "X"),
      (id: 2, name: "A"),
      (id: 1, name: "X"),
    ]
    let tupleDataExpected = @[
      (key: (id: 2, name: "A"), count: 1),
      (key: (id: 1, name: "X"), count: 1),
      (key: (id: 2, name: "X"), count: 2),
      (key: (id: 1, name: "A"), count: 3),
    ]

    check DF.fromSeq(tupleDataDuplicates)
            .valueCounts()
            .sort(x => (x.count, x.key.name))
            .collect() == tupleDataExpected
