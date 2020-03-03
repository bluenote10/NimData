import nimdata
import nimdata/utils
import math

UnitTestSuite("Reduce/Fold Actions"):
  test "reduce":
    check DF.fromSeq(@[1, 2, 3]).reduce((a, b) => a + b) == 6
    check DF.fromSeq(@[1, 2, 3]).reduce((a, b) => max(a, b)) == 3
    check DF.fromSeq(@[1, 2, 3]).reduce((a, b) => min(a, b)) == 1

  test "fold":
    check DF.fromSeq(@[1, 2, 3]).fold(0.0, (a, b) => a + b.float) == 6.0
    check DF.fromSeq(@[1, 2, 3]).fold(-Inf, (a, b) => max(a, b.float)) == 3.0
    check DF.fromSeq(@[1, 2, 3]).fold(+Inf, (a, b) => min(a, b.float)) == 1.0
    check DF.fromSeq(@[1, 2, 3]).fold("", (a, b) => a & $b) == "123"


UnitTestSuite("Numerical Actions"):
  test "sum":
    check DF.fromSeq(@[1, 2, 3]).sum() == 6
    check DF.fromSeq(@[1, 2, 3]).map(x => x).sum() == 6
    check DF.fromSeq(@[1, 2, 3]).filter(_ => false).sum() == 0

  test "mean":
    check DF.fromSeq(@[1, 2, 3]).mean() == 2
    check DF.fromSeq(@[1, 2, 3]).map(x => x).mean() == 2
    check DF.fromSeq(@[1, 2, 3]).filter(_ => false).mean().classify() == fcNaN

  test "min":
    check DF.fromSeq(@[1, 2, 3]).min() == 1
    check DF.fromSeq(@[1, 2, 3]).map(x => x).min() == 1
    check DF.fromSeq(@[1, 2, 3]).filter(_ => false).min() == high(int)
    check DF.fromSeq(@[-1, -2, -3]).min() == -3
    check DF.fromSeq(@[+1.0, +2.0, +3.0]).min() == 1.0
    check DF.fromSeq(@[-1.0, -2.0, -3.0]).min() == -3.0

  test "max":
    check DF.fromSeq(@[1, 2, 3]).max() == 3
    check DF.fromSeq(@[1, 2, 3]).map(x => x).max() == 3
    check DF.fromSeq(@[1, 2, 3]).filter(_ => false).max() == low(int)
    check DF.fromSeq(@[-1, -2, -3]).max() == -1
    check DF.fromSeq(@[+1.0, +2.0, +3.0]).max() == 3.0
    check DF.fromSeq(@[-1.0, -2.0, -3.0]).max() == -1.0

  test "median":
    check DF.fromSeq(@[1,2,3]).median() == 2.0
    check DF.fromSeq(@[1,2,3,4]).median() == 2.5
    check DF.fromSeq(@[-1,-2,-3]).median() == -2.0
    check DF.fromSeq(@[-1,-2,-3,-4]).median() == -2.5

  test "population stdev":
    check DF.fromSeq(@[1, 2, 3]).stdev() == 0.816496580927726
    check DF.fromSeq(@[1, 2, 3]).map(x => x).stdev() == 0.816496580927726
    check DF.fromSeq(@[1.5, 2.5, 2.5, 2.75, 3.25, 4.75]).stdev() == 0.986893273527251

  test "sample stdev":
    check DF.fromSeq(@[1, 2, 3]).stdev(ddof = 1) == 1.0
    check DF.fromSeq(@[1, 2, 3]).map(x => x).stdev(ddof = 1) == 1.0
    check DF.fromSeq(@[1.5, 2.5, 2.5, 2.75, 3.25, 4.75]).stdev(ddof = 1) == 1.0810874155219827
