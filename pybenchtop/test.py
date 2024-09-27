

import pybenchtop


d = pybenchtop.Driver("test.data")
print(d)

t = d.new("table_1", {"column_1":float})

print(t)

#t.Close()