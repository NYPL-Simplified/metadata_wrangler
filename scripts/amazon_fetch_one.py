"""Fetch data from Amazon about one identifier."""

import os
import site
import sys
import isbnlib
from nose.tools import set_trace
d = os.path.split(__file__)[0]
site.addsitedir(os.path.join(d, ".."))

from integration.amazon import (
    AmazonAPI,
)

from model import(
    production_session,
    Identifier,
)

if __name__ == '__main__':
    if len(sys.argv) < 3:
        print "Usage: %s [data storage directory] [ASIN]" % sys.argv[0]
        sys.exit()
    path = sys.argv[1]      
    asin = sys.argv[2]
    if isbnlib.is_isbn10(asin):
        type = Identifier.ISBN
    else:
        type = Identifier.ASIN
    db = production_session()
    identifier, ignore = Identifier.for_foreign_id(db, type, asin)
    api = AmazonAPI(db)
    print api.fetch_bibliographic_info(identifier)
    print
    for review in api.fetch_reviews(identifier):
        print review
        print "-" * 80
    db.commit()
