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
    WorkRecord,
)
from database_credentials import SERVER, MAIN_DB

if __name__ == '__main__':
    session = SessionManager.session(SERVER, MAIN_DB)

    if len(sys.argv) > 1 and sys.argv[1] == 'delete':
        print "Deleting all works."
        update = dict(work_id=None)
        session.query(WorkRecord).filter(WorkRecord.work_id!=None).update(update)
        session.query(LicensePool).filter(LicensePool.work_id!=None).update(update)
        session.query(Work).delete()
        session.commit()

    print "Creating new works."
    LicensePool.consolidate_works(session)
    session.commit()
