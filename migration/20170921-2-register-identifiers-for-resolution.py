#!/usr/bin/env python
"""Registers unresolved identifiers with all their possible CoverageProviders"""

import os
import sys
import logging
from nose.tools import set_trace
from sqlalchemy import func, and_
from sqlalchemy.orm import lazyload

bin_dir = os.path.split(__file__)[0]
package_dir = os.path.join(bin_dir, "..")
sys.path.append(os.path.abspath(package_dir))

from core.model import (
    Collection,
    CoverageRecord,
    DataSource,
    Identifier,
    collections_identifiers,
    production_session,
)

from coverage import (
    IdentifierResolutionCoverageProvider,
    IdentifierResolutionRegistrar,
)

log = logging.getLogger(name="Metadata Wrangler identifier registration migration")

class MockResolver(IdentifierResolutionCoverageProvider):
    """This Mock ResolutionCoverageProvider avoids creating all of the APIs,
    which won't be necessary for this migration.
    """
    def __init__(self, collection):
        super(IdentifierResolutionCoverageProvider, self).__init__(
            collection, preregistered_only=True
        )

try:
    _db = production_session()
    registrar = IdentifierResolutionRegistrar(_db)

    log.info('Finding unresolved identifiers')
    data_source = DataSource.lookup(_db, DataSource.INTERNAL_PROCESSING)
    unresolved_qu = Identifier.missing_coverage_from(
        _db, [], data_source,
        operation=CoverageRecord.RESOLVE_IDENTIFIER_OPERATION,
        count_as_covered=CoverageRecord.SUCCESS
    ).filter(CoverageRecord.id != None)

    log.info('Finding unaffiliated identifiers without a collection')
    unresolved_and_unaffiliated = unresolved_qu.outerjoin(Identifier.collections)\
        .group_by(Identifier.id).having(func.count(Collection.id)==0)\
        .options(lazyload(Identifier.licensed_through)).distinct()

    if unresolved_and_unaffiliated.count() > 1:
        # Use a bulk insert to add them all to the unaffiliated_collection.
        log.info('Giving all unaffiliated identifiers a collection')
        unaffiliated_collection, ignore = MockResolver.unaffiliated_collection(_db)
        _db.execute(
            collections_identifiers.insert(),
            [
                dict(
                    collection_id=unaffiliated_collection.id,
                    identifier_id=identifier.id
                ) for identifier in unresolved_and_unaffiliated
            ]
        )
        _db.commit()

    # Get an IdentifierResolutionCoverageProvider for each collection.
    resolvers = MockResolver.all(_db)

    # Only get identifiers that haven't had any work done yet with
    # other CoverageProviders.
    identifiers_with_one_coverage_record = unresolved_qu\
        .group_by(Identifier.id).having(func.count(CoverageRecord.id)==1)\
        .with_entities(Identifier.id).subquery()

    # Now register them for all the CoverageProviders they'll ever need.
    for resolver in resolvers:
        identifiers = resolver.items_that_need_coverage().filter(
            Identifier.id.in_(identifiers_with_one_coverage_record)
        )
        for identifier in identifiers:
            registrar.register(identifier, force=True)
        _db.commit()

except Exception as e:
    log.error('%r', e, exc_info=e)
finally:
    _db.close()
