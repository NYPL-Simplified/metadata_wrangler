import os
import site
import sys
import json
import re
from nose.tools import set_trace
from collections import defaultdict
d = os.path.split(__file__)[0]
site.addsitedir(os.path.join(d, ".."))
from integration.illustrated import GutenbergIllustratedDriver
from model import (
    production_session,
)

if __name__ == '__main__':
    if len(sys.argv) < 2:
        print "Usage: %s [path to ls-R]" % sys.argv[0]
        sys.exit()
    path = sys.argv[1]      
    db = production_session()
    for data in GutenbergIllustratedDriver.data_from_file_list(db, open(path)):
        print data
        
