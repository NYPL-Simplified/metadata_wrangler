import calendar
import os
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
from integration.threem import ThreeMAPI
from integration import FilesystemCache

def imp(db, data_source, path, url):
    modified = from_timestamp(os.stat(path).st_mtime)
    data = open(path).read()
    representation, ignore = get_one_or_create(db, Representation,
        url=url, data_source=data_source)
    representation.status_code = 200
    representation.content = data
    representation.media_type = 'application/xml'
    representation.fetched_at = modified
    print(url)

if __name__ == '__main__':
    data_dir = sys.argv[1]
    
    template = "http://cloudlibraryapi.3m.com/cirrus/library/a4tmf/data/cloudevents?startdate=%s&enddate=%s"

    db = production_session()
    threem = DataSource.lookup(db, DataSource.THREEM)

    cache_path = os.path.join(data_dir, "3M", "cache", "events")
    a = 0
    for filename in os.listdir(cache_path):
        path = os.path.join(cache_path, filename)
        start_date = filename[:19]
        end_date = filename[20:]
        url = template % (start_date, end_date)
        imp(db, threem, path, url)
        a += 1
        if not a % 10:
            db.commit()
    db.commit()
