#!/usr/bin/env python

from __future__ import division, print_function

import pandas as pd
import numpy as np


def gen_data(filename, N, num_int_cols=2, num_float_cols=2):

    data = {}

    for col in xrange(num_int_cols):
        data["int_col_{}".format(col)] = np.random.randint(0, 100, size=N)

    for col in xrange(num_float_cols):
        data["float_col_{}".format(col)] = np.random.uniform(0, 1, size=N)

    df = pd.DataFrame(data)
    df.to_csv(filename, index=False, header=False)


if __name__ == '__main__':
    np.random.seed(0)
    gen_data("test_01.csv", N=1*1000*1000)
