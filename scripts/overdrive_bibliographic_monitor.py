import os
import site
import sys
d = os.path.split(__file__)[0]
site.addsitedir(os.path.join(d, ".."))
from integration.overdrive import (
    OverdriveBibliographicMonitor,
)
from model import production_session

if __name__ == '__main__':
    session = production_session()
    OverdriveBibliographicMonitor(session).run()
