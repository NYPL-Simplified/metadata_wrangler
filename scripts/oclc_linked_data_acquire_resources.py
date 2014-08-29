import os
import site
import sys
d = os.path.split(__file__)[0]
site.addsitedir(os.path.join(d, ".."))
from integration.oclc import (
    LinkedDataCoverageProvider,
)
from model import (
    production_session,
    CoverageProvider,
    DataSource,
    Work,
)
from nose.tools import set_trace


if __name__ == '__main__':
    if len(sys.argv) < 2:
        print "Usage: %s [data storage directory]" % sys.argv[0]
        sys.exit()
    path = sys.argv[1]      
    LinkedDataCoverageProvider(production_session(), path).run()
