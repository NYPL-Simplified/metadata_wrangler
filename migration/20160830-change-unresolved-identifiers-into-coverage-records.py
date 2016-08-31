#!/usr/bin/env python
"""Turn UnresolvedIdentifier objects into appropriate CoverageRecords"""

import datetime
import logging
import os
import sys
bin_dir = os.path.split(__file__)[0]
package_dir = os.path.join(bin_dir, "..")
sys.path.append(os.path.abspath(package_dir))

from nose.tools import set_trace
from core.model import (
    get_one_or_create,
    production_session,
    BaseCoverageRecord,
    CoverageRecord,
    DataSource,
    UnresolvedIdentifier,
)
from controller import URNLookupController

_db = production_session()
source = DataSource.lookup(_db, DataSource.INTERNAL_PROCESSING)
unresolved_identifiers = _db.query(UnresolvedIdentifier).all()

print "Replacing %d UnresolvedIdentifiers with CoverageRecords" % len(unresolved_identifiers)

for unresolved in unresolved_identifiers:
    identifier = unresolved.identifier
    record, is_new = get_one_or_create(
        _db, CoverageRecord,
        identifier=identifier, data_source=source,
        operation=CoverageRecord.RESOLVE_IDENTIFIER_OPERATION
    )

    if is_new:
        # This CoverageRecord wasn't created from a lookup prior to this
        # migration, so it should duplicate as much of the
        # UnresolvedIdentifier's data as it can.
        record.timestamp = unresolved.most_recent_attempt
        record.status = BaseCoverageRecord.TRANSIENT_FAILURE

        record.exception = unresolved.exception
        if not record.exception:
            # The UnresolvedIdentifier didn't have an exception, so this
            # identifier hadn't been run at all yet.
            # We'll give it the default lookup message.
            record.exception = URNLookupController.NO_WORK_DONE_EXCEPTION

        print "Resolution Coverage Record created for %r" % identifier
    else:
        print "Resolution Coverage Record already exists for %r" % identifier
_db.commit()
