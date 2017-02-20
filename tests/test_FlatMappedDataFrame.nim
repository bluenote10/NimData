import nimdata
import nimdata/utils
import strutils

UnitTestSuite("FlatMappedDataFrame"):
  test "Construction":
    let data = DF.fromSeq(@[1, 3, 5])

    proc plusOneDuplicator(x: int): (iterator(): int) =
      result = iterator(): int =
        echo "yielding ", x
        yield x
        echo "yielding ", x + 1
        yield x + 1

    check data.flatMap(plusOneDuplicator).collect() == @[1, 2, 3, 4, 5, 6]