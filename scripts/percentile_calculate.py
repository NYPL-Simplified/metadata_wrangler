import sys
import json
import numpy

data = []

for i in sys.stdin:
    data.append(int(i.strip()))

percentiles = []
data = sorted(data)
for i in range(0,100):
    percentiles.append(int(numpy.percentile(data, i)))
print json.dumps(percentiles)
