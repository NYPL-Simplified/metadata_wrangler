"""Gather up LicensePool objects into Work objects."""

import os
import site
import sys
from nose.tools import set_trace
d = os.path.split(__file__)[0]
site.addsitedir(os.path.join(d, ".."))

from model import (
    DataSource,
    LicensePool,
    SessionManager,
    Work,
    WorkGenre,
    WorkIdentifier,
    WorkRecord,
)
from model import production_session

if __name__ == '__main__':
    session = production_session()

    force = False
    works_from_source = None
    if len(sys.argv) == 2:
        if sys.argv[1] == 'force':
            force = True
            works_from_source = DataSource.lookup(session, sys.argv[2])
        else:
            works_from_source = DataSource.lookup(session, sys.argv[1])
            force = False

    if len(sys.argv) == 4:
        source, type, id = sys.argv[1:]
        pool, ignore = LicensePool.for_foreign_id(session, source, type, id)
        work = pool.work
    else:
        work = None
    
    if work:
        print "Recalculating presentation for %s" % work
        work.calculate_presentation()
    else:
        if works_from_source:
            which_works = works_from_source.name
        else:
            which_works = "all"

        print "Recalculating presentation for %s works, force=%r" % (
            which_works, force)
        i = 0
        q = session.query(Work)
        if works_from_source:
            q = q.outerjoin(WorkRecord)
        if not force:
            q = q.outerjoin(WorkGenre).filter(WorkGenre.id==None).filter(Work.fiction==None).filter(Work.audience==None)

        if works_from_source:
            q = q.filter(WorkRecord.data_source==works_from_source)

        print "That's %d works." % q.count()
        for work in q:
            work.calculate_presentation()
            i += 1
            if not i % 10:
                session.commit()
    session.commit()

