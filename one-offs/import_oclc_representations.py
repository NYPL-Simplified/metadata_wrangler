from nose.tools import set_trace
import calendar
import os
import json
import site
import sys
import time
import datetime
d = os.path.split(__file__)[0]
site.addsitedir(os.path.join(d, ".."))
from model import (
    production_session,
    DataSource,
    Identifier,
    Representation,
    get_one_or_create,
)
from integration.oclc import (
    OCLCLinkedData,
    OCLCCache,
)

def imp(db, data_source, identifier, cache):
    i = identifier.identifier
    type = identifier.type
    key = (i, type)
    if not cache.exists(key):
        return
    fn = cache._filename(key)
    modified = datetime.datetime.fromtimestamp(os.stat(fn).st_mtime)
    data = open(fn).read()

    location = None
    status_code = 200
    media_type = "application/ld+json"
    if type == Identifier.OCLC_WORK:
        url = OCLCLinkedData.WORK_BASE_URL % dict(id=i, type="work")
    elif type == Identifier.OCLC_NUMBER:
        url = OCLCLinkedData.BASE_URL % dict(id=i, type="oclc")
    elif type == Identifier.ISBN:
        url = OCLCLinkedData.ISBN_BASE_URL % dict(id=i)
        location = data
        data = None
        media_type = None
        status_code = 301
    representation, ignore = get_one_or_create(
        db, Representation,
        url=url, data_source=data_source, identifier=identifier,
        )
    representation.status_code = status_code
    representation.content = data
    representation.location = location
    representation.media_type = media_type
    representation.fetched_at = modified


if __name__ == '__main__':
    data_dir = sys.argv[1]

    oclc = OCLCLinkedData(data_dir)
    b = oclc.cache
    db = production_session()

    source = DataSource.lookup(db, DataSource.OCLC_LINKED_DATA)
    q = db.query(Identifier).filter(Identifier.type.in_(
        [Identifier.OCLC_WORK, Identifier.OCLC_NUMBER, Identifier.ISBN]))
    start = 0
    keep_going = True
    while keep_going:
        keep_going = False
        for identifier in q.offset(start).limit(start+1000):
            imp(db, source, identifier, b)
            print identifier
            keep_going = True
        start += 1000
        db.commit()
