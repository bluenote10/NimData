#!/usr/bin/env python

from __future__ import division, print_function

import pandas
import dask.dataframe as dd
import time


test_file = "test_01.csv"


def run_timed(name, func, numRepeats=3):
    runtimes = []
    for i in xrange(numRepeats):
        print("\n *** Running {} [Iteration: {}]".format(name, i+1))
        t1 = time.time()
        func()
        t2 = time.time()
        runtimes.append(t2 - t1)
    runtimes = pandas.Series(runtimes)
    return "{:<40s}    min: {:6.3f}    mean: {:6.3f}    max: {:6.3f}".format(
        name,
        runtimes.min(),
        runtimes.mean(),
        runtimes.max()
    )


def load_df():
    # the testfile is ~ 35 MB using blocksize of 10 should give
    # get good parallelism on 4 cores. Going for a larger number
    # of partitions seems to slow down again.
    df = dd.read_csv(test_file, blocksize=int(10e6), header=None, names=["A", "B", "C", "D"])
    return df


def test_count():
    df = load_df()
    print("Number of partitions:", df.npartitions)
    print(len(df))


def test_column_averages():
    df = load_df()
    meanA = df.A.mean().compute()
    meanB = df.B.mean().compute()
    meanC = df.C.mean().compute()
    meanD = df.D.mean().compute()
    print(meanA, meanB, meanC, meanD)


result_strings = [
    run_timed(
        "Count",
        test_count
    ),
    run_timed(
        "Column averages",
        test_column_averages
    ),
]

print("\n *** Summary:")
print("\n".join(result_strings))
