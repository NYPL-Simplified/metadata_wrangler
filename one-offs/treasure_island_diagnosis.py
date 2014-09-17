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
db = production_session()

# The Overdrive side:

# 97743 Overdrive ID/b05cda64-e444-4749-8b7c-6cc7b7b374be ID=97743 wr=69270 ("Treasure Island")
# 223246 Overdrive ID/5e63b751-6f89-4260-9d93-760c5dcf33b8 ID=223246 wr=135402 ("Treasure Island")

# The Gutenberg side:

# 2382 Gutenberg ID/120 ID=2382 wr=2378 ("Treasure Island")
# 15522 Gutenberg ID/23936 ID=15522 wr=15489 ("Treasure Island")
# 19790 Gutenberg ID/27780 ID=19790 wr=19752 ("Treasure Island")

# OCLC weighs in:

# 85789 OCLC Work ID/3434 ID=85789 wr=57315 ("Treasure Island")
# 85791 OCLC Work ID/425239673 ID=85791 wr=57317 ("Treasure Island")
# 85800 OCLC Work ID/1862602312 ID=85800 wr=57326 ("Treasure Island")
# 85808 OCLC Work ID/1151753349 ID=85808 wr=57334 ("Treasure Island")

overdrive_ids = []
gutenberg_ids = []
oclc_ids = []

for i in db.query(WorkRecord).filter(WorkRecord.title=="Treasure Island"):
    if i.data_source.name == "Overdrive":
        bucket = overdrive_ids
    elif i.data_source.name == "Gutenberg":
        bucket = gutenberg_ids
    elif i.data_source.name == "OCLC Classify":
        bucket = oclc_ids
    bucket.append(i.primary_identifier)

m = WorkIdentifier.recursively_equivalent_identifier_ids_flat
equivalent_to_overdrive = set(m(db, overdrive_ids))
equivalent_to_gutenberg = set(m(db, gutenberg_ids))
equivalent_to_oclc = set(m(db, oclc_ids))

print "Overdrive: %d=>%s" % (len(overdrive_ids), len(equivalent_to_overdrive))
print "Gutenberg: %d=>%s" % (len(gutenberg_ids), len(equivalent_to_gutenberg))
print "OCLC: %d=>%s" % (len(oclc_ids), len(equivalent_to_oclc))

#for i in db.query(WorkIdentifier).filter(WorkIdentifier.id.in_(equivalent_to_gutenberg)):
#    print i

print equivalent_to_overdrive.intersection(equivalent_to_gutenberg)
