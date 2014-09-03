"""Gather up LicensePool objects into Work objects."""

import os
import site
import sys
from nose.tools import set_trace
d = os.path.split(__file__)[0]
site.addsitedir(os.path.join(d, ".."))

from model import (
    SessionManager,
    Work,
    WorkGenre,
)
from model import production_session

if __name__ == '__main__':
    session = production_session()

    force = False
    if len(sys.argv) > 1 and sys.argv[1] == 'force':
        force = True

    print "Recalculating presentation for all works, force=%r" % force
    i = 0
    q = session.query(Work)
    if not force:
        q = q.outerjoin(WorkGenre).filter(WorkGenre.id==None)
    for work in q:
        work.calculate_presentation()
        if not work.title:
            set_trace()
        i += 1
        if not i % 10:
            session.commit()
    session.commit()

