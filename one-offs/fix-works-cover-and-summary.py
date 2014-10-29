import os
import site
import sys
import datetime
from nose.tools import set_trace
d = os.path.split(__file__)[0]
site.addsitedir(os.path.join(d, ".."))
from model import (
    production_session,
    Work
)
from sqlalchemy.orm import joinedload

a = 0
db = production_session()
start = 0
batch_size = 1000
base_query = db.query(Work).order_by(Work.id).options(
        joinedload('summary'), joinedload('primary_edition', 'cover')).limit(batch_size)
batch = base_query.offset(start).all()
while batch:
    for work in batch:
        if work.primary_edition and work.primary_edition.cover:
            work.primary_edition.set_cover(work.primary_edition.cover)
        if work.summary:
            work.set_summary(work.summary)
    db.commit()
    start += batch_size
    batch = base_query.offset(start).all()
db.commit()
