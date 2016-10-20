#!/usr/bin/env python
"""Turn resolved Identifiers into appropriate CoverageRecords"""

import datetime
import logging
import os
import sys
bin_dir = os.path.split(__file__)[0]
package_dir = os.path.join(bin_dir, "..")
sys.path.append(os.path.abspath(package_dir))

from nose.tools import set_trace
from sqlalchemy import or_
from sqlalchemy.orm import lazyload

from core.model import (
    get_one_or_create,
    production_session,
    BaseCoverageRecord,
    CoverageRecord,
    DataSource,
    Identifier,
    UnresolvedIdentifier,
)

_db = production_session()
source = DataSource.lookup(_db, DataSource.INTERNAL_PROCESSING)

covered = _db.query(CoverageRecord.id).select_from(CoverageRecord).\
    join(DataSource).filter(DataSource.name==source.name)
covered = covered.subquery()

# Get all of the Identifiers that can be resolved by the Metadata Wrangler
# (ISBNs and Overdrive IDs only). Select only the ones that do not have
# an either an UnresolvedIdentifier or a resolve-identifier CoverageRecord.
resolved_identifiers = _db.query(Identifier).\
    outerjoin(Identifier.unresolved_identifier).\
    filter(Identifier.type.in_([Identifier.ISBN, Identifier.OVERDRIVE_ID])).\
    filter(UnresolvedIdentifier.id==None).\
    filter(~Identifier.coverage_records.any(CoverageRecord.id.in_(covered))).\
    options(lazyload(Identifier.licensed_through)).all()

print "%d resolved Identifiers require coverage" % len(resolved_identifiers)

batch_size = 50
index = 0
while index < len(resolved_identifiers):
    batch = resolved_identifiers[index:index+batch_size]
    print "Resolving next batch of %d Identifiers" % len(batch)

    for identifier in batch:
        record, is_new = get_one_or_create(
            _db, CoverageRecord,
            identifier=identifier, data_source=source,
            operation=CoverageRecord.RESOLVE_IDENTIFIER_OPERATION
        )

        if is_new:
            # This CoverageRecord wasn't created from a lookup prior to this
            # migration, so this is a successfully resolved identifier from
            # the time of UnresolvedIdentifiers.
            record.status = BaseCoverageRecord.SUCCESS
            print "Resolution Coverage Record created for %r" % identifier
        else:
            print "Resolution Coverage Record already exists for %r" % identifier
    _db.commit()
    index += batch_size
