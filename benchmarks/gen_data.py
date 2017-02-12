#!/usr/bin/env python

from __future__ import division, print_function

import pandas as pd
import numpy as np
import time


class TimedContext(object):

    def __init__(self, context):
        self.context = context

    def __enter__(self):
        print("Running: " + self.context + "...")
        self.t1 = time.time()

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.t2 = time.time()
        print("Time: {}".format(self.t2 - self.t1))


def gen_data(N=1*1000*1000, num_int_cols=2, num_float_cols=2):

    data = {}

    for col in xrange(num_int_cols):
        data["int_col_{}".format(col)] = np.random.randint(0, 100, size=N)

    for col in xrange(num_int_cols):
        data["float_col_{}".format(col)] = np.random.uniform(0, 1, size=N)

    df = pd.DataFrame(data)
    df.to_csv("test_01.csv", index=False, header=False)


# gen_data()
test_file = "test_01.csv"

with TimedContext("Count rows (Python)"):
    count = 0
    for _ in open(test_file).readlines():
        count += 1
    print(count)

with TimedContext("Count rows (Pandas)"):
    df = pd.read_csv(test_file, header=None, names=["A", "B", "C", "D"])
    print(len(df))

with TimedContext("Compute means (Pandas)"):
    df = pd.read_csv(test_file, header=None, names=["A", "B", "C", "D"])
    meanA = df.A.mean()
    meanB = df.B.mean()
    meanC = df.C.mean()
    meanD = df.D.mean()
    print(meanA, meanB, meanC, meanD)
