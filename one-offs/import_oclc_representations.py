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
        print "Already did", identifier
        return False

    print "Checking", identifier
    key = (i, type)
    if not cache.exists(key):
        print "Not cached", identifier
        return False
    fn = cache._filename(key)
    modified = datetime.datetime.fromtimestamp(os.stat(fn).st_mtime)
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

    oclc = OCLCLinkedData(data_dir)
    b = oclc.cache
    db = production_session()

    source = DataSource.lookup(db, DataSource.OCLC_LINKED_DATA)
    #types = [Identifier.OCLC_WORK, Identifier.OCLC_NUMBER, Identifier.ISBN]
    types = [Identifier.ISBN]

    #all_ids = [x.id for x in db.query(Identifier).join(Representation).filter(
    #    Identifier.type.in_(types)).filter(Representation.data_source==source)]
    #q = db.query(Identifier).filter(~Identifier.id.in_(all_ids)).order_by(Identifier.id).filter(Identifier.type.in_(types))
    #print "Excluding", len(all_ids)
    #q = db.query(Identifier).outerjoin(
    #    Representation,
    #    (Identifier.id==Representation.identifier_id
    #     and Representation.data_source_id==source.id)
    # ).filter(Identifier.type.in_(types)).filter(Representation.id==None).order_by(Identifier.id)
    q = db.query(Identifier).filter(Identifier.type.in_(types)).order_by(Identifier.id)
    start = 0
    batch_size = 10000
    keep_going = True
    while keep_going:
        keep_going = False
        processed = 0
        for identifier in q.offset(start).limit(start+batch_size):
            keep_going = True
            if imp(db, source, identifier, b):
                processed += 1
        start += batch_size
        if processed > 0:
            db.commit()
