"""Gather up LicensePool objects into Work objects."""

import os
import site
import sys
from nose.tools import set_trace
d = os.path.split(__file__)[0]
site.addsitedir(os.path.join(d, ".."))

from sqlalchemy.sql.functions import func
from model import (
    DataSource,
    LicensePool,
    SessionManager,
    Work,
    WorkGenre,
    Edition,
)
from model import production_session

if __name__ == '__main__':
    session = production_session()

    force = False
    works_from_source = None
    if len(sys.argv) == 2:
        works_from_source = DataSource.lookup(session, sys.argv[1])
    if works_from_source:
        which_works = works_from_source.name
    else:
        which_works = "all"

    print "Reclassifying %s works." % (which_works)
    i = 0
    q = session.query(Work)
    if works_from_source:
        q = q.join(Edition).filter(Edition.data_source==works_from_source)
    q = q.order_by(func.random())

    print "That's %d works." % q.count()
    for work in q:
        genres = work.genres
        work.calculate_presentation(
            choose_edition=False, classify=True, choose_summary=False, calculate_quality=False,
            debug=True)
        new_genres = work.genres
        if new_genres != genres:
            set_trace()
        i += 1
        if not i % 10:
            session.commit()
    session.commit()

