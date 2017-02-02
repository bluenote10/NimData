
import future

import ../../src/nimdata
import ../../src/nimdata_utils

#let df = newFileRowsDataFrame("../test_01.csv").map(x => x)
let df = newPersistedDataFrame(@["A", "B", "C"])

let data = df.collect()
echo data.len
