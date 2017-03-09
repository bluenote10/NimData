#!/usr/bin/env python

from __future__ import division, print_function

import pandas as pd
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
    runtimes = pd.Series(runtimes)
    return "{:<40s}    min: {:6.3f}    mean: {:6.3f}    max: {:6.3f}".format(
        name,
        runtimes.min(),
        runtimes.mean(),
        runtimes.max()
    )


def test_count_python():
    count = 0
    for _ in open(test_file).readlines():
        count += 1
    print(count)


def test_count_pandas():
    df = pd.read_csv(test_file, header=None, names=["A", "B", "C", "D"])
    print(len(df))


def test_column_averages():
    df = pd.read_csv(test_file, header=None, names=["A", "B", "C", "D"])
    meanA = df.A.mean()
    meanB = df.B.mean()
    meanC = df.C.mean()
    meanD = df.D.mean()
    print(meanA, meanB, meanC, meanD)


def test_unique_values1():
    df = pd.read_csv(test_file, header=None, names=["A", "B", "C", "D"])
    count = df.C.nunique()
    print(count)


def test_unique_values2():
    df = pd.read_csv(test_file, header=None, names=["A", "B", "C", "D"])
    count = len(df[["C", "D"]].drop_duplicates())
    print(count)


def test_join():
    df_a = pd.read_csv("test_02_a.csv", header=None, names=["K1", "K2", "K3", "valA"])
    df_b = pd.read_csv("test_02_b.csv", header=None, names=["K1", "K2", "K3", "valB"])
    joined = df_a.merge(df_b, on=["K1", "K2", "K3"])
    mean_diff = (joined["valA"] - joined["valB"]).mean()
    print(mean_diff)


result_strings = [
    run_timed(
        "Count (no parsing, pure Python)",
        test_count_python
    ),
    run_timed(
        "Count (Pandas)",
        test_count_pandas
    ),
    run_timed(
        "Column averages",
        test_column_averages
    ),
    run_timed(
        "Unique values 1",
        test_unique_values1
    ),
    run_timed(
        "Unique values 2",
        test_unique_values2
    ),
    run_timed(
        "Join",
        test_join
    ),
]

print("\n *** Summary:")
print("\n".join(result_strings))