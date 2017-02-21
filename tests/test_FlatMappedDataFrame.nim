import nimdata
import nimdata/utils
import strutils

UnitTestSuite("FlatMappedDataFrame"):
  test "flatMap":
    let data = DF.fromSeq(@[1, 3, 5])

    proc plusOneDuplicator(x: int): (iterator(): int) =
      result = iterator(): int =
        yield x
        yield x + 1

    proc emptyIter(x: int): (iterator(): int) =
      result = iterator(): int =
        discard

    proc convertingIter(x: int): (iterator(): string) =
      result = iterator(): string =
        yield $x

    check data.flatMap(plusOneDuplicator).collect() == @[1, 2, 3, 4, 5, 6]
    check data.flatMap(emptyIter).collect() == newSeq[int]()
    check data.flatMap(convertingIter).collect() == @["1", "3", "5"]
