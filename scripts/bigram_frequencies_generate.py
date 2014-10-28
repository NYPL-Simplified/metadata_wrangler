from nose.tools import set_trace
import os
import site
import sys
import csv
import string
import isbnlib
import json
d = os.path.split(__file__)[0]
site.addsitedir(os.path.join(d, ".."))

from util import Bigrams

bigrams = Bigrams.from_text_files(sys.argv[1:])
print json.dumps(bigrams.proportional, sort_keys=True, indent=4)



