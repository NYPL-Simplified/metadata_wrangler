from nose.tools import set_trace
import os
import site
import sys
d = os.path.split(__file__)[0]
site.addsitedir(os.path.join(d, ".."))

import datetime
from model import (
    DataSource,
    CoverageRecord,
    production_session,
    Identifier,
    Measurement,
    LicensePool,
    Edition
)

min_availability_date = datetime.datetime(2014, 04, 01)
max_availability_date = datetime.datetime(2014, 11, 12)

db = production_session()

m = datetime.time(0,0,0)

for e in db.query(Edition).filter(Edition.work_id != None).order_by(Edition.id.desc()):
    availability = min_availability_date
    if e.issued:
        a = datetime.datetime.combine(e.issued, m)
    elif e.published:
        a = datetime.datetime.combine(e.published, m)
    else:
        a = availability

    if a >= availability and a <= max_availability_date:
        availability = a

    pool = e.license_pool
    if pool:
        print e.title, availability
        pool.availability_date = availability
    else:
        print "No license pool for %s" % e.title
