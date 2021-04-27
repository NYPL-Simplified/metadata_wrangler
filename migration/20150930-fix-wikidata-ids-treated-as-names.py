#!/usr/bin/env python
"""Recalculate the display information about all contributors
mistakenly given Wikidata IDs as 'names'.
"""

from pdb import set_trace
import os
import sys
bin_dir = os.path.split(__file__)[0]
package_dir = os.path.join(bin_dir, "..")
sys.path.append(os.path.abspath(package_dir))

import re
from core.model import (
    production_session, 
    Contributor, 
)
from viaf import VIAFClient

_db = production_session()
viaf_client = VIAFClient(_db)
from sqlalchemy.sql import text
contributors = _db.query(Contributor).filter(
    text("contributors.display_name ~ '^Q[0-9]'")
).order_by(Contributor.id)
print(contributors.count())
for contributor in contributors:
    if contributor.viaf:
        viaf, display_name, family_name, sort_name, wikipedia_name = viaf_client.lookup_by_viaf(contributor.viaf)
    else:
        viaf, display_name, family_name, sort_name, wikipedia_name = viaf_client.lookup_by_name(contributor.name)
    print("%s: %s => %s, %s => %s" % (
        contributor.id, 
        contributor.display_name, display_name,
        contributor.wikipedia_name, wikipedia_name
    ))
    contributor.display_name = display_name
    contributor.wikipedia_name = wikipedia_name
    contributor.family_name = family_name
    viaf, display_name, family_name, sort_name, wikipedia_name = viaf_client.lookup_by_viaf(contributor.viaf)
    for contribution in contributor.contributions:
        edition = contribution.edition
        if edition.work:
            edition.work.calculate_presentation()
        else:
            edition.calculate_presentation()
    _db.commit()

