import sys
import re
from collections import Counter

bigrams = Counter()
all_letters = re.compile("^[a-z]+$")

def process_file(filename):
    data = open(filename).read()
    for i in range(0, len(data)-1):
        bigram = data[i:i+2].strip()
        if len(bigram) == 2 and all_letters.match(bigram):
            bigrams[bigram.lower()] += 1

for filename in sys.argv[1:]:
    process_file(filename)

total = float(sum(bigrams.values()))
for bigram, quantity in bigrams.most_common():
    proportion = quantity/total
    if proportion < 0.001:
        break
    print bigram, proportion

