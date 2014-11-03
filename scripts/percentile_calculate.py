import sys
import json
import numpy

data = []

reverse = False
if len(sys.argv) > 1 and sys.argv[1] == 'low is good':
    reverse = True

for i in sys.stdin:
    v = i.strip()
    if not v:
        continue
    v = int(v)
    if reverse:
        v = -v
    data.append(v)

percentiles = []
data = sorted(data)
for i in range(0,100):
    percentiles.append(int(numpy.percentile(data, i)))

if reverse:
    percentiles = [-x for x in percentiles]

print json.dumps(percentiles)
