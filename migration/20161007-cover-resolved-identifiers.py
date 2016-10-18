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

# Get all of the Identifiers that can be resolved by the Metadata Wrangler
# (ISBNs and Overdrive IDs only). Select only the ones that do not have
# an either an UnresolvedIdentifier or a resolve-identifier CoverageRecord.
resolved_identifiers = _db.query(Identifier).\
    filter(Identifier.type.in_([Identifier.ISBN, Identifier.OVERDRIVE_ID])).\
    outerjoin(Identifier.coverage_records).\
    outerjoin(CoverageRecord.data_source).\
    filter(or_(CoverageRecord.id==None, DataSource.name!=source.name)).\
    outerjoin(Identifier.unresolved_identifier).\
    filter(UnresolvedIdentifier.id==None).\
    options(lazyload(Identifier.licensed_through)).all()

print "Resolving %d Identifiers with CoverageRecords" % len(resolved_identifiers)

for identifier in resolved_identifiers:
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
