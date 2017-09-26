#!/usr/bin/env python
"""Add a Collection.external_account_id and DataSource for OPDS_IMPORT
collections.
"""
import base64
import os
import sys
import logging
from nose.tools import set_trace

bin_dir = os.path.split(__file__)[0]
package_dir = os.path.join(bin_dir, "..")
sys.path.append(os.path.abspath(package_dir))

from core.model import (
    Collection,
    ConfigurationSetting,
    DataSource,
    ExternalIntegration,
    production_session,
)

log = logging.getLogger(name="Metadata Wrangler configuration import")

try:
    _db = production_session()

    # Get all of the OPDS_IMPORT collections.
    collections = Collection.by_protocol(_db, ExternalIntegration.OPDS_IMPORT)

    for collection in collections:
        opds_url = collection.external_account_id
        if not opds_url:
            decoded_collection, ignore = Collection.from_metadata_identifier(
                _db, collection.name
            )

            opds_url = decoded_collection.external_account_id
            if not opds_url:
                # This shouldn't happen.
                log.warn(
                    'Could not find external_account_id for %r' % collection
                )
                continue
            if opds_url and collection == decoded_collection:
                log.info(
                    'Added URL "%s" to collection %r',
                    decoded_collection.external_account_id, decoded_collection
                )
            else:
                # Somehow the collection has been duplicated. This shouldn't
                # happen, but if it does, we shouldn't update the collection's
                # data_source on faulty information.
                opds_url = None

        if opds_url and 'librarysimplified.org' in opds_url:
            collection.data_source = DataSource.OA_CONTENT_SERVER
    _db.commit()
except Exception as e:
    log.error('%r', e, exc_info=e)
finally:
    _db.close()
