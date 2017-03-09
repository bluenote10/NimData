#!/usr/bin/env python

from __future__ import division, print_function

import pandas as pd
import numpy as np
import itertools


def gen_data(filename, N, num_int_cols=2, num_float_cols=2):

    data = {}

    for col in xrange(num_int_cols):
        data["int_col_{}".format(col)] = np.random.randint(0, 100, size=N)

    for col in xrange(num_float_cols):
        data["float_col_{}".format(col)] = np.random.uniform(0, 1, size=N)

    df = pd.DataFrame(data)
    df.to_csv(filename, index=False, header=False)


def gen_key_df(cube_sizes=[200, 200, 200]):

    ids = [
        np.random.randint(0, 1000000, size=size)
        for size in cube_sizes
    ]

    df = pd.DataFrame(list(itertools.product(*ids)))
    return df


def gen_data_joinable(filename, key_df, num_float_cols=1):

    df = key_df.copy()

    for col in xrange(num_float_cols):
        df["float_col_{}".format(col)] = np.random.uniform(0, 1, size=len(df))

    df = df.sample(frac=1).reset_index(drop=True)
    df.to_csv(filename, index=False, header=False)


if __name__ == '__main__':
    np.random.seed(0)

    gen_data("test_01.csv", N=1*1000*1000)

    key_df = gen_key_df()
    gen_data_joinable("test_02_a.csv", key_df)
    gen_data_joinable("test_02_b.csv", key_df)
