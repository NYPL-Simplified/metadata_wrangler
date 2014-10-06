"""Scrape one  objects into Work objects."""

import os
import site
import sys
from nose.tools import set_trace
d = os.path.split(__file__)[0]
site.addsitedir(os.path.join(d, ".."))

from integration.amazon import (
    AmazonScraper,
)

if __name__ == '__main__':
    if len(sys.argv) < 3:
        print "Usage: %s [data storage directory] [ASIN]" % sys.argv[0]
        sys.exit()
    path = sys.argv[1]      
    asin = sys.argv[2]
    AmazonScraper(path).scrape_reviews(asin)
