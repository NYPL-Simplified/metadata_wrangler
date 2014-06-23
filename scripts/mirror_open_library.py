import os
import site
import sys
d = os.path.split(__file__)[0]
site.addsitedir(os.path.join(d, ".."))
from integration.openlibrary import (
    OpenLibraryMonitor
)
from model import production_session

if __name__ == '__main__':
    if len(sys.argv) < 2:
        print "Usage: %s [data storage directory]" % sys.argv[0]
        sys.exit()

    path = sys.argv[1]      
    OpenLibraryMonitor().mirror_all(production_session(), path)
