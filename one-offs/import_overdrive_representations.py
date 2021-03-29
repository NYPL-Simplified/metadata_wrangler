import calendar
import os
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
from integration.overdrive import OverdriveAPI

def imp(db, data_source, identifier, cache, library):
    i = identifier.identifier
    fn = i + ".json"
    if not cache.exists(fn):
        return
    fn = cache._filename(fn)
    modified = datetime.datetime.fromtimestamp(os.stat(fn).st_mtime)
    data = cache.open(fn).read()
    a = dict(collection_token=library['collectionToken'],
             item_id=i)
    url = OverdriveAPI.METADATA_ENDPOINT % a
    representation, ignore = get_one_or_create(db, Representation,
        url=url, data_source=data_source, identifier=identifier)
    representation.status_code = 200
    representation.content = data
    representation.media_type = 'application/json'
    representation.fetched_at = modified
    print(identifier)

if __name__ == '__main__':
    data_dir = sys.argv[1]

    overdrive = OverdriveAPI(data_dir)
    library = overdrive.get_library()
    db = production_session()
    b = overdrive.bibliographic_cache

    source = DataSource.lookup(db, DataSource.OVERDRIVE)
    q = db.query(Identifier).filter(Identifier.type==Identifier.OVERDRIVE_ID)
    a = 0
    for i in q:
        imp(db, source, i, b, library)
        a += 1
        if not a % 1000:
            db.commit()
