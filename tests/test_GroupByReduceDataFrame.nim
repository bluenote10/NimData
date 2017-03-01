import nimdata
import nimdata/utils
import strutils

UnitTestSuite("GroupByReduceDataFrame"):
  test "groupBy":

    let data = @[
      (id: 1, name: "A"),
      (id: 1, name: "A"),
      (id: 2, name: "X"),
      (id: 1, name: "A"),
      (id: 2, name: "X"),
      (id: 2, name: "A"),
      (id: 1, name: "X"),
    ]
    let df = DF.fromSeq(data)

    proc keyFunc(x: tuple[id: int, name: string]): int = x.id
    proc reduceFunc(key: int, df: DataFrame[tuple[id: int, name: string]]): int =
      result = df.count()

    # general compiler tests
    block:
      let gb = df.groupBy(x => x.id, (key, df) => df.count())
    block:
      let gb = groupBy(df, x => x.id, (key, df) => df.count())
    block:
      let gb = groupBy(df, keyFunc, reduceFunc)
    block:
      let gb = groupBy[tuple[id: int, name: string], int, int](df, keyFunc, reduceFunc)

    # These error with (not quite sure why, should be the right types?):
    # Error: A nested proc can have generic parameters only when it is used as an operand to another routine and the types of the generic paramers can be inferred from the expected signature.
    # let gb = groupBy[tuple[id: int, name: string], int, int](df, x => x.id, (key, df) => df.count())
    # let gb = groupBy[tuple[id: int, name: string], int, int](df, x => x.id, reduceFunc)

    # grouping by id
    block:
      let expected = @[3, 4]
      check:
        df.groupBy(
            x => x.id,
            (key, df) => df.count()
          )
          .sort()
          .collect() == expected
    block:
      let expected = @[
        (key: 1, count: 4),
        (key: 2, count: 3)
      ]
      check:
        df.groupBy(
            x => x.id,
            (key, df) => (key: key, count: df.count())
          )
          .sort(x => x.key)
          .collect() == expected
    block:
      let expected = @[
        (key: 1, reduced: "AAAX"),
        (key: 2, reduced: "XXA")
      ]
      check:
        df.groupBy(
            x => x.id,
            (key, df) => (key: key, reduced: df.map(x => x.name).reduce((a, b) => a & b))
          )
          .sort(x => x.key)
          .collect() == expected

    # grouping by name
    block:
      let expected = @[3, 4]
      check:
        df.groupBy(
            x => x.name,
            (key, df) => df.count()
          )
          .sort()
          .collect() == expected
    block:
      let expected = @[
        (key: "A", count: 4),
        (key: "X", count: 3)
      ]
      check:
        df.groupBy(
            x => x.name,
            (key, df) => (key: key, count: df.count())
          )
          .sort(x => x.key)
          .collect() == expected
    block:
      let expected = @[
        (key: "A", reduced: 5),
        (key: "X", reduced: 5)
      ]
      check:
        df.groupBy(
            x => x.name,
            (key, df) => (key: key, reduced: df.map(x => x.id).reduce((a, b) => a + b))
          )
          .sort(x => x.key)
          .collect() == expected
