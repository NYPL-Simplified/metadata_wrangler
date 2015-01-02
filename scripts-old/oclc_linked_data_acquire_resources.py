import os
import site
import sys
from multiprocessing import Pool, Process
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


def f(services):
    print "Starting coverage provider"
    LinkedDataCoverageProvider(production_session(), services).run()

if __name__ == '__main__':
    if len(sys.argv) > 1:
        services = sys.argv[1:]
    else:
        services = None
    f(services)
    #for i in range(20):
    #    p = Process(target=f, args=(services,))
    #    p.start()
