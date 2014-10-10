from nose.tools import set_trace
import calendar
import os
import site
import sys
import time
import datetime
d = os.path.split(__file__)[0]
site.addsitedir(os.path.join(d, ".."))
from integration.amazon import (
    AmazonCoverageProvider,
)
from model import (
    production_session,
    DataSource,
    Identifier,
    Representation,
    get_one_or_create,
)
from integration.threem import ThreeMAPI

def imp(db, data_source, identifier, cache):
    i = identifier.identifier
    if not cache.exists(i):
        return
    fn = cache._filename(i)
    modified = datetime.datetime.fromtimestamp(os.stat(fn).st_mtime)
    data = cache.open(fn).read()
    url = "http://cloudlibraryapi.3m.com/cirrus/library/items/%s" % i
    representation, ignore = get_one_or_create(db, Representation,
        url=url, data_source=data_source, identifier=identifier)
    representation.status_code = 200
    representation.content = data
    representation.media_type = 'application/xml'
    representation.fetched_at = modified
    print identifier


if __name__ == '__main__':
    data_dir = sys.argv[1]

    threem = ThreeMAPI(os.path.join(data_dir, "3M"))
    db = production_session()
    b = threem.bibliographic_cache

    source = DataSource.lookup(db, DataSource.THREEM)
    q = db.query(Identifier).filter(Identifier.type==Identifier.THREEM_ID)
    a = 0
    for i in q:
        imp(db, source, i, b)
        a += 1
        if not a % 1000:
            db.commit()
