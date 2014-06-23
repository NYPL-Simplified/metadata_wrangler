import os
import site
import sys
d = os.path.split(__file__)[0]
site.addsitedir(os.path.join(d, ".."))
from integration.gutenberg import (
    GutenbergAPI,
    GutenbergMonitor,
)
from model import production_session

if __name__ == '__main__':
    if len(sys.argv) < 2:
        print "Usage: %s [data storage directory]" % sys.argv[0]
        sys.exit()
    path = sys.argv[1]      
    session = production_session()
    GutenbergMonitor(path).run(session)
