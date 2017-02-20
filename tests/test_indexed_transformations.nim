import nimdata
import nimdata/utils

UnitTestSuite("Indexed Transformations"):
  test "take":
    let data = DF.fromSeq(@[1, 2, 3]).take(2)
    check data.count() == 2
    check data.collect() == @[1, 2]
    check data.cache().count() == 2
    check data.map(x => x).count() == 2
    check data.filter(x => true).count() == 2

    check DF.fromSeq(@[1, 2, 3]).take(0).count() == 0
    check DF.fromSeq(@[1, 2, 3]).take(1).count() == 1
    check DF.fromSeq(@[1, 2, 3]).take(2).count() == 2
    check DF.fromSeq(@[1, 2, 3]).take(3).count() == 3
    check DF.fromSeq(@[1, 2, 3]).take(4).count() == 3

    check DF.fromSeq(@[1, 2, 3]).take(2).collect() == @[1, 2]
    check DF.fromSeq(@[1, 2, 3]).take(2).take(2).collect() == @[1, 2]
    check DF.fromSeq(@[1, 2, 3]).take(2).take(2).take(2).collect() == @[1, 2]

  test "drop":
    let data = DF.fromSeq(@[1, 2, 3]).drop(2)
    check data.count() == 1
    check data.collect() == @[3]
    check data.cache().count() == 1
    check data.map(x => x).count() == 1
    check data.filter(x => true).count() == 1

    check DF.fromSeq(@[1, 2, 3]).drop(0).count() == 3
    check DF.fromSeq(@[1, 2, 3]).drop(1).count() == 2
    check DF.fromSeq(@[1, 2, 3]).drop(2).count() == 1
    check DF.fromSeq(@[1, 2, 3]).drop(3).count() == 0
    check DF.fromSeq(@[1, 2, 3]).drop(4).count() == 0

    check DF.fromSeq(@[1, 2, 3]).drop(1).collect() == @[2, 3]
    check DF.fromSeq(@[1, 2, 3]).drop(1).drop(1).collect() == @[3]
    check DF.fromSeq(@[1, 2, 3]).drop(1).drop(1).drop(1).collect() == newSeq[int]()
    check DF.fromSeq(@[1, 2, 3]).drop(1).drop(1).drop(1).drop(1).collect() == newSeq[int]()

  test "filterWithIndex":
    check DF.fromSeq(@[1, 2, 3]).filterWithIndex((i, x) => i == 1).collect() == @[2]

  test "mapWithIndex":
    check DF.fromSeq(@[1, 2, 3]).mapWithIndex((i, x) => i == 1).collect() == @[false, true, false]
    check DF.fromSeq(@[1, 2, 3]).mapWithIndex((i, x) => i*x).collect() == @[0, 2, 6]
