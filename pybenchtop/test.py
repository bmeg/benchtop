

import pybenchtop


d = pybenchtop.Driver("test.data")
print(d)

t = d.new("table_1", {"column_1":float})

print(t)

print(t.add("key1", {"column_1": 0.9, "column_2": 1.2}))

print(t.get("key1"))

d.close()