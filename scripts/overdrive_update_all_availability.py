import os
import site
import sys
d = os.path.split(__file__)[0]
site.addsitedir(os.path.join(d, ".."))
from model import (
    DataSource,
    production_session,
    Identifier,
)
from integration.overdrive import OverdriveAPI

if __name__ == '__main__':
    if len(sys.argv) < 2:
        print "Usage: %s [data storage directory]" % sys.argv[0]
        sys.exit()
    path = sys.argv[1]      
    _db = production_session()
    overdrive = OverdriveAPI(path)
    overdrive_source = DataSource.lookup(_db, DataSource.OVERDRIVE)
    for identifier in _db.query(Identifier).filter(
            Identifier.type==overdrive_source.primary_identifier_type):
        overdrive.update_licensepool(
            _db, overdrive_source, identifier.identifier)
