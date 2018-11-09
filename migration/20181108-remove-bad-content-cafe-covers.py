#!/usr/bin/env python
#
# Stop works from considering covers from Content Cafe which turned out to be
# placeholders.

import os
import sys
bin_dir = os.path.split(__file__)[0]
package_dir = os.path.join(bin_dir, "..")
sys.path.append(os.path.abspath(package_dir))

from core.s3 import *
from core.config import Configuration
from core.model import (
    production_session,
    Representation,
    get_one,
    PresentationCalculationPolicy,
)

_db = production_session()
qu = _db.query(Representation).filter(
    Representation.image_height==120).filter(
        Representation.image_width==80).filter(
            Representation.url.like("http://contentcafe2.btol.com/%")
        ).order_by(Representation.id)
policy = PresentationCalculationPolicy(
    regenerate_opds_entries=True, classify=False,
    choose_summary=False, calculate_quality=False
)
for rep in qu:
    print rep.id
    identifiers = [h.identifier for h in rep.resource.links]
    fix_editions = []
    for identifier in identifiers:
        print identifier
        for edition in identifier.primarily_identifies:
            if (edition.cover_thumbnail_url and 'Content' in edition.cover_thumbnail_url) or (edition.cover_full_url and 'Content' in edition.cover_full_url):
                fix_editions.append(edition)

    # Delete the hyperlinks so we don't use these images anymore.
    for h in rep.resource.links:
        _db.delete(h)

    # Wipe out the cover image URLs for all the editions associated
    # with this identifier that need fixing, and recalculate their works'
    # presentations.
    for edition in fix_editions:
        edition.cover_thumbnail_url = edition.cover_full_url = None
        if edition.work:
            edition.work.calculate_presentation(policy)
    _db.commit()
