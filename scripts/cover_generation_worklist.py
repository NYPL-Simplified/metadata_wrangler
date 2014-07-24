"""Print information for all the Gutenberg covers that need to be generated."""

import json
import os
import site
import sys
from nose.tools import set_trace
d = os.path.split(__file__)[0]
site.addsitedir(os.path.join(d, ".."))

from model import (
    LicensePool,
    SessionManager,
    Work,
    WorkRecord,
    WorkIdentifier,
)
from model import production_session

if __name__ == '__main__':
    session = production_session()

    books = []
    i = 0
    for r in session.query(
            WorkRecord).join(WorkRecord.primary_identifier).filter(
            WorkIdentifier.type == WorkIdentifier.GUTENBERG_ID):
        identifier = r.primary_identifier
        filename = "%s/%s.png" % (identifier.type, identifier.identifier)
        data = dict(
            identifier_type=identifier.type, identifier=identifier.identifier,
            filename = filename, title=r.title,
            subtitle=r.subtitle,
            authors=", ".join([x.name for x in r.authors])
        )
        books.append(data)
    print json.dumps(dict(books=data))
