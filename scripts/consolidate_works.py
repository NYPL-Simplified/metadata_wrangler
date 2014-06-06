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

    print "Deleting all works."
    #for work in session.query(Work).all():
    #    session.delete(work)

    print "Creating new works."
    LicensePool.consolidate_works(session)
    session.commit()
