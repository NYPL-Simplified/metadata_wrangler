"""Create LicensePools for all IDs found in the bibliographic monitor cache."""

import os
import site
import datetime
import sys
d = os.path.split(__file__)[0]
site.addsitedir(os.path.join(d, ".."))
from integration.overdrive import (
    OverdriveCirculationMonitor,
)
from model import production_session

class ImportIntoCirculation(OverdriveCirculationMonitor):
    def __init__(self, path):
        super(ImportIntoCirculation, self).__init__(path)
        self.ids = []

    def recently_changed_ids(self, start, cutoff):
        self.stop_running = True
        return self.ids

if __name__ == '__main__':
    if len(sys.argv) < 2:
        print "Usage: %s [data storage directory]" % sys.argv[0]
        sys.exit()
    path = sys.argv[1]      

    session = production_session()
    importer = ImportIntoCirculation(path)
    ids = [x.strip() for x in open(os.path.join(importer.path, "seed_ids.list"))]
    importer.ids = ids
    importer.run(session)
