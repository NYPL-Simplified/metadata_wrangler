import os
import site
import sys
import datetime
d = os.path.split(__file__)[0]
site.addsitedir(os.path.join(d, ".."))
from integration.threem import (
    ThreeMEventMonitor,
)
from model import production_session

DEFAULT_START_TIME = datetime.datetime(2012, 8, 15)

if __name__ == '__main__':
    if len(sys.argv) < 2:
        print "Usage: %s [data storage directory]" % sys.argv[0]
        sys.exit()
    path = sys.argv[1]      
    session = production_session()
    ThreeMEventMonitor(path, DEFAULT_START_TIME).run(session)
