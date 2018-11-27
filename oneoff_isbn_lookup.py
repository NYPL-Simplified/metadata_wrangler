import sys
from oclc.classify import (
    IdentifierLookupCoverageProvider
)

from core.model import (
    production_session,
    Identifier,
)

_db = production_session()

isbn = sys.argv[1]
identifier, is_new = Identifier.for_foreign_id(_db, Identifier.ISBN, isbn)

provider = IdentifierLookupCoverageProvider(_db)
provider.process_item(identifier)
