from nose.tools import set_trace
import os
import site
import sys
d = os.path.split(__file__)[0]
site.addsitedir(os.path.join(d, ".."))
from integration.amazon import (
    AmazonCoverageProvider,
)
from model import (
    production_session,
)

if __name__ == '__main__':
    if len(sys.argv) < 2:
        print "Usage: %s [data storage directory]" % sys.argv[0]
        sys.exit()
    path = sys.argv[1]
    if len(sys.argv) > 2:
        types = sys.argv[2:]
    else:
        types = None
    AmazonCoverageProvider(production_session(), path, types).run()
