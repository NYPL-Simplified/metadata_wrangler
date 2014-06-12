import os
import site
import sys
d = os.path.split(__file__)[0]
site.addsitedir(os.path.join(d, ".."))
from integration.openlibrary import (
    OpenLibraryMonitor
)
from model import SessionManager
from database_credentials import SERVER, MAIN_DB

if __name__ == '__main__':
    if len(sys.argv) < 2:
        print "Usage: %s [data storage directory]" % sys.argv[0]
        sys.exit()

    path = sys.argv[1]      
    path = os.path.join(
        path, "Open Library", "oclc_coverids_2011-03-31.txt")

    session = SessionManager.session(SERVER, MAIN_DB)
    OpenLibraryMonitor().run(session, path)
