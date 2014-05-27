"""Gather up LicensePool objects into EText objects."""

import os
import site
import sys
d = os.path.split(__file__)[0]
site.addsitedir(os.path.join(d, ".."))

from model import SessionManager, LicensePool
from database_credentials import SERVER, MAIN_DB

if __name__ == '__main__':
    session = SessionManager.session(SERVER, MAIN_DB)
    LicensePool.assign_missing_to_etexts(session)
