"""Use Gutenberg Illustrated to generate covers and upload them to S3."""
import os
import site
import sys
from nose.tools import set_trace
d = os.path.split(__file__)[0]
site.addsitedir(os.path.join(d, ".."))

from model import production_session
from integration.illustrated import GutenbergIllustratedCoverageProvider

if __name__ == '__main__':
    if len(sys.argv) < 3:
        print "Usage: %s [data directory] [path to Gutenberg Illustrated binary]" % sys.argv[0]
        sys.exit()
    data_directory, binary_path = sys.argv[1:]
    _db = production_session()
    GutenbergIllustratedCoverageProvider(_db, data_directory, binary_path).run()
