import os
import site
import sys
d = os.path.split(__file__)[0]
site.addsitedir(os.path.join(d, ".."))
from integration.viaf import (
    VIAFClient,
)
from model import production_session

if __name__ == '__main__':
    if len(sys.argv) < 2:
        print "Usage: %s [data storage directory] [force]" % sys.argv[0]
        sys.exit()
    path = sys.argv[1]      
    force = False
    if len(sys.argv) > 2 and sys.argv[2] == 'force':
        force = True
    VIAFClient(production_session(), path).run(force)
