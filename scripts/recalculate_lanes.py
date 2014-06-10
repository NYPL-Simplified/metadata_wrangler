"""Gather up LicensePool objects into Work objects."""

import os
import site
import sys
d = os.path.split(__file__)[0]
site.addsitedir(os.path.join(d, ".."))

from model import (
    LicensePool,
    SessionManager,
    Work,
)
from database_credentials import SERVER, MAIN_DB

if __name__ == '__main__':
    session = SessionManager.session(SERVER, MAIN_DB)

    print "Recalculating lanes for all works."
    i = 0
    for work in session.query(Work):
        work.calculate_lane()
        work.calculate_presentation(session)
        # print repr(work)
        i += 1
        if not i % 10:
            session.commit()
    session.commit()
