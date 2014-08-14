"""Processes OCLC Linked Data for all works in the system.

Outputs to a file the OCLC Linked Data URL for each edition it finds.

wget can then get these edition URLs very quickly:

 $ cd "$DATA/OCLC Linked Data/cache/oclc"
 $ wget -nc --header "Accept: application/ld+json" --input-file=$URL_LIST

Doing this before running LinkedDataCoverageProvider for the first
time will save a huge amount of time.
"""
import os
import site
import sys
d = os.path.split(__file__)[0]
site.addsitedir(os.path.join(d, ".."))
from integration.oclc import (
    LinkedDataURLLister,
)
from model import (
    production_session,
    CoverageProvider,
    DataSource,
    Work,
)
from nose.tools import set_trace

if __name__ == '__main__':
    if len(sys.argv) < 3:
        print "Usage: %s [data storage directory] [destination path for URL list]" % sys.argv[0]
        sys.exit()
    path = sys.argv[1]
    url_list_path = sys.argv[2]
    LinkedDataURLLister(production_session(), path, url_list_path).run()
