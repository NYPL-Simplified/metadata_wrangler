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
    WorkRecord,
    WorkIdentifier,
)
from model import production_session

if __name__ == '__main__':
    session = production_session()

    gutenberg_id = sys.argv[1]
    gutenberg = DataSource.lookup(session, DataSource.GUTENBERG)
    wid, ignore = WorkIdentifier.for_foreign_id(
        session, WorkIdentifier.GUTENBERG_ID, gutenberg_id, False)
    pool = session.query(LicensePool).filter(
        LicensePool.data_source==gutenberg).filter(
            LicensePool.identifier==wid).one()
    pool.calculate_work()
    session.commit()
