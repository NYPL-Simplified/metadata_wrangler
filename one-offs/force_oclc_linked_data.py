import isbnlib
import os
import site
import sys
import json
d = os.path.split(__file__)[0]
site.addsitedir(os.path.join(d, ".."))

from nose.tools import set_trace

from collections import Counter, defaultdict
from model import (
    DataSource,
    WorkRecord,
    Work,
    WorkIdentifier,
    production_session,
)
from integration.oclc import LinkedDataCoverageProvider
db = production_session()

wr = db.query(WorkRecord).filter(WorkRecord.primary_identifier_id==85808).one()

# 85789
#85791
# 85800
p = LinkedDataCoverageProvider(db, "/home/leonardr/data/")

p.process_work_record(wr)

db.commit()
