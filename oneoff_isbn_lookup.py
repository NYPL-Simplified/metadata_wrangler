import sys
from oclc.classify import (
    IdentifierLookupCoverageProvider
)

from core.lane import Lane
from core.model import (
    Collection,
    production_session,
    Identifier,
)

_db = production_session()

isbn = sys.argv[1]
identifier, is_new = Identifier.for_foreign_id(_db, Identifier.ISBN, isbn)
collection, is_new = Collection.by_name_and_protocol(_db, name="a", protocol="b")
provider = IdentifierLookupCoverageProvider(collection=collection)
provider.process_item(identifier)
