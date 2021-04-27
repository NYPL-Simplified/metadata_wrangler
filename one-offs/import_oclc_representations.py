import calendar
import os
import json
import site
import sys
import time
import datetime
d = os.path.split(__file__)[0]
site.addsitedir(os.path.join(d, ".."))
from core.utils.datetime_helpers import from_timestamp
from model import (
    production_session,
    DataSource,
    Identifier,
    Representation,
    get_one_or_create,
)
from integration import FilesystemCache
from integration.oclc import (
    OCLCLinkedData,
)

class FakeCache(FilesystemCache):
    def _filename(self, key):
        return super(FakeCache, self)._filename(key) + ".jsonld"

def imp(db, data_source, identifier, cache):
    i = identifier.identifier
    type = identifier.type

    location = None
    status_code = 200
    media_type = "application/ld+json"
    if type == Identifier.OCLC_WORK:
        url = OCLCLinkedData.WORK_BASE_URL % dict(id=i, type="work")
    elif type == Identifier.OCLC_NUMBER:
        url = OCLCLinkedData.BASE_URL % dict(id=i, type="oclc")
    elif type == Identifier.ISBN:
        url = OCLCLinkedData.ISBN_BASE_URL % dict(id=i)
        media_type = None
        status_code = 301
    representation, new = get_one_or_create(
        db, Representation,
        url=url, data_source=data_source, identifier=identifier,
        )
    if not new:
        print("Already did", identifier)
        return False

    if not cache.exists(i):
        # print "Not cached", identifier
        return False
    fn = cache._filename(i)
    modified = from_timestamp(os.stat(fn).st_mtime)
    data = open(fn).read()

    if type == Identifier.ISBN:
        location = data
        data = None

    representation.status_code = status_code
    representation.content = data
    representation.location = location
    representation.media_type = media_type
    representation.fetched_at = modified
    return True

if __name__ == '__main__':
    data_dir = sys.argv[1]

    db = production_session()
    oclc = OCLCLinkedData(db)
    d = os.path.join(data_dir, "OCLC Linked Data", "cache", "OCLC Number")
    cache = FakeCache(d, 4, False)

    source = DataSource.lookup(db, DataSource.OCLC_LINKED_DATA)
    min_oclc = 1284796
    max_oclc = 2052405
    batch_size = 10000
    type = Identifier.OCLC_NUMBER

    cursor = min_oclc
    while cursor < max_oclc:
        first_time = time.time()
        processed = 0
        max_batch = cursor + batch_size
        q = db.query(Identifier).filter(Identifier.type==Identifier.OCLC_NUMBER).filter(Identifier.id >= cursor).filter(Identifier.id < max_batch)

        for identifier in q:
            if imp(db, source, identifier, cache):
                processed += 1
        if processed > 0:
            a = "%d sec, %d cached/%d, final" % (time.time()-first_time, processed, batch_size)
            print(a, identifier)
            db.commit()
        cursor = max_batch
