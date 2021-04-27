import os
import site
import sys
import datetime
d = os.path.split(__file__)[0]
site.addsitedir(os.path.join(d, ".."))
from integration.threem import (
    ThreeMAPI,
)
from integration.overdrive import (
    OverdriveAPI,
)

from model import (
    production_session,
    DataSource,
    Edition,
    Identifier,
)

if __name__ == '__main__':
    type, identifier_name = sys.argv[1:3]
    db = production_session()
    identifier, is_new = Identifier.for_foreign_id(db, type, identifier_name)
    if identifier.type==Identifier.THREEM_ID:
        source = DataSource.lookup(db, DataSource.THREEM)
        api = ThreeMAPI(db)
        edition, ignore = Edition.for_foreign_id(
            db, source, type, identifier_name)
        data = api.get_bibliographic_info_for([edition])

