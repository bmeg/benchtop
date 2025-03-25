

import numpy as np
import pybenchtop

dbpath = "test.table"

dr = pybenchtop.Driver(dbpath)
table = dr.new("VECTORS", {"embedding" : pybenchtop.VECTOR})

for i in range(100):
    table.add(str(i), np.random.rand(256))

dr.close()
