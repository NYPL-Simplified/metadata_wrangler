"""Explain groupings of Works."""

import os
import site
import sys
from pdb import set_trace
d = os.path.split(__file__)[0]
site.addsitedir(os.path.join(d, ".."))

from model import (
    LicensePool,
    SessionManager,
    Work,
)
from database_credentials import SERVER, MAIN_DB

if __name__ == '__main__':
    session = SessionManager.session(SERVER, MAIN_DB)

    if len(sys.argv) > 1:
        cutoff = float(sys.argv[1])
    else:
        cutoff = 0

    for work in session.query(Work):
        if len(work.license_pools) < 2:
            continue
        print "Work #%d: %s, by %s (%s)" % (
            work.id, work.title, work.authors, work.languages)

        by_similarity = []
        for record in work.work_records:
            similarity = work.similarity_to(record)
            if similarity >= cutoff:
                by_similarity.append((similarity, record))
        by_similarity = sorted(by_similarity, reverse=True)
        print " Has claimed %s work records (%d shown)" % (
            len(work.work_records), len(by_similarity))
        for similarity, record in by_similarity:
            print "  %.3f %r" % (similarity, record)

        print " Has claimed %s license pools" % len(work.license_pools)
        for pool in work.license_pools:
            wr = pool.work_record(session)
            print " License pool for %r" % wr
            records = [wr] + wr.equivalent_work_records(session)

            by_similarity = []
            for record in records:
                similarity = wr.similarity_to(record)
                if similarity >= cutoff:
                    by_similarity.append((similarity, record))
            by_similarity = sorted(by_similarity, reverse=True)
            print "  Associated with %s work records (%d shown)" % (
                len(records), len(by_similarity))
            for similarity, record in by_similarity:
                print "   %.3f %r" % (similarity, record)
