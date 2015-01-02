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
    if len(sys.argv) > 1:
        types = sys.argv[1:]
    else:
        types = None

    AmazonCoverageProvider(production_session(), types).run()
