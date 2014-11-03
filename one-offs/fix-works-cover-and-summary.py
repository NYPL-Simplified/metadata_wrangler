import os
import site
import sys
import datetime
from nose.tools import set_trace
d = os.path.split(__file__)[0]
site.addsitedir(os.path.join(d, ".."))
from model import (
    Edition,
    production_session,
    DataSource,
    Work
)
from sqlalchemy.orm import joinedload

a = 0
db = production_session()
start = 0
batch_size = 1000
source = DataSource.lookup(db, DataSource.THREEM)
base_query = db.query(Work).join(Work.primary_edition).filter(Edition.data_source==source).order_by(Work.id).options(
        joinedload('summary'), joinedload('primary_edition', 'cover')).limit(batch_size)
batch = base_query.offset(start).all()
while batch:
    for work in batch:
        if not work.primary_edition:
            continue
        if work.primary_edition.cover:
            work.primary_edition.set_cover(work.primary_edition.cover)
            print work.primary_edition.cover_thumbnail_url
        else:
            print "!COVER %s" % work.primary_edition.primary_identifier
        if work.summary:
            work.set_summary(work.summary)
            print work.summary.content[:70]
    db.commit()
    start += batch_size
    batch = base_query.offset(start).all()
db.commit()
