"""Gather up LicensePool objects into Work objects."""

import os
import site
import sys
d = os.path.split(__file__)[0]
site.addsitedir(os.path.join(d, ".."))

from model import (
    DataSource,
    LicensePool,
    SessionManager,
    Work,
    Identifier,
)
from model import production_session

if __name__ == '__main__':
    session = production_session()

    data_source_name = sys.argv[1]
    identifier = sys.argv[2]
    data_source = DataSource.lookup(session, data_source_name)
    wid, ignore = Identifier.for_foreign_id(
        session, data_source.primary_identifier_type, identifier, False)
    pool = session.query(LicensePool).filter(
        LicensePool.data_source==data_source).filter(
            LicensePool.identifier==wid).one()
    primary_edition = pool.edition()
    old_work = primary_edition.work
    if old_work:
        old_work.license_pools.remove(pool)
    primary_edition.work = None
    pool.calculate_work()
    work = pool.work
    work.calculate_presentation()
    session.commit()
