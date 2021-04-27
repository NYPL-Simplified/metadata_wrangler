import os
import site
import sys
d = os.path.split(__file__)[0]
site.addsitedir(os.path.join(d, ".."))

import zlib
import csv
import datetime
from collections import defaultdict

from model import (
    CirculationEvent,
    DataSource,
    CoverageRecord,
    production_session,
    Identifier,
    Measurement,
    LicensePool,
)
import json
import gzip
from core.util.datetime_helpers import strptime_utc

database = production_session()
data_dir = sys.argv[1]
OVERDRIVE = DataSource.lookup(database, DataSource.OVERDRIVE)

TIME_FORMAT = "%Y-%m-%dT%H:%M:%S+00:00"

def process_item(_db, item):
                              
    overdrive_id = item['id']
    event_name = item['event']
    old_value = item.get('old_value', 0)
    new_value = item.get('new_value', 0)
    if event_name in ('check_out', 'check_in'):
        x = new_value
        new_value = old_value
        old_value = x
    elif event_name in ('hold_release', 'hold_place', 'license_remove'):
        pass
    elif event_name in ('title_add'):
        old_value = new_value = None
    start = strptime_utc(item['start'], TIME_FORMAT)
    pool, is_new = LicensePool.for_foreign_id(_db, OVERDRIVE, Identifier.OVERDRIVE_ID,
                                      overdrive_id)
    if is_new:
        CirculationEvent.log(
            _db, pool, CirculationEvent.TITLE_ADD, None, None, start=start)
        pool.availability_time = start
    if event_name == 'title_add':
        pool.availability_time = start
    CirculationEvent.log(_db, pool, event_name, old_value, new_value, start)

def process_file(_db, filename):
    try:
        for i in gzip.open(filename):
            data = json.loads(i.strip())
            process_item(_db, data)
    except zlib.error as e:
        print("DATA CORRUPTION, GIVING UP ON THIS FILE")
    except IOError as e:
        print("DATA CORRUPTION, GIVING UP ON THIS FILE")


done = set()
done_path = os.path.join(data_dir, "done")
if os.path.exists(done_path):
    for i in open(done_path):
        done.add(i.strip())

done_out = open(done_path, "a")
for filename in os.listdir(data_dir):
    if not filename.endswith(".log.gz"):
        continue
    path = os.path.join(data_dir, filename)
    if path in done:
        print("Already did %s" % path)
        continue
    process_file(database, path)
    print("DONE with %s! DONE!" % path)
    database.commit()
    done_out.write(path + "\n")
