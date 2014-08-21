"""Gather up LicensePool objects into Work objects."""

import os
import site
import sys
from nose.tools import set_trace
d = os.path.split(__file__)[0]
site.addsitedir(os.path.join(d, ".."))

from model import (
    LicensePool,
    SessionManager,
    Work,
)
from model import production_session

if __name__ == '__main__':
    session = production_session()

    print "Recalculating lanes for all works."
    i = 0
    for work in session.query(Work):
        work.calculate_presentation()
        if not work.title:
            set_trace()
        i += 1
        if not i % 10:
            session.commit()
    session.commit()

