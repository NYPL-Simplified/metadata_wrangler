"""Gather up LicensePool objects into Work objects."""

import os
import site
import sys
d = os.path.split(__file__)[0]
site.addsitedir(os.path.join(d, ".."))

from nose.tools import set_trace

from model import (
    DataSource,
    LicensePool,
    Work,
    WorkGenre,
    Identifier,
    Edition,
    production_session,
)

if __name__ == '__main__':
    session = production_session()

    if len(sys.argv) > 1 and sys.argv[1] == 'delete':
        if len(sys.argv) > 2:
            delete_work_type = sys.argv[2]
            print "Deleting all %s works." % delete_work_type
            data_source = DataSource.lookup(session, delete_work_type)
            identifier_type = data_source.primary_identifier_type
        else:
            data_source = None
            print "Deleting all works."
            
        update = dict(work_id=None)

        work_ids_to_delete = set()

        work_records = session.query(Edition)
        if data_source:
            work_records = work_records.join(
                Identifier).filter(
                    Identifier.type==identifier_type)
            for wr in work_records:
                work_ids_to_delete.add(wr.work_id)
            work_records = session.query(Edition).filter(
                Edition.work_id.in_(work_ids_to_delete))
        else:
            work_records = work_records.filter(Edition.work_id!=None)
        work_records.update(update, synchronize_session='fetch')

        pools = session.query(LicensePool)
        if data_source:
            pools = pools.join(Identifier).filter(
                Identifier.type==identifier_type)
            if data_source:
                for pool in pools:
                    # This should not be necessary--every single work ID we're
                    # going to delete should have showed up in the first
                    # query--but just in case.
                    work_ids_to_delete.add(pool.work_id)
            pools = session.query(LicensePool).filter(
                LicensePool.work_id.in_(work_ids_to_delete))
        else:
            pools = pools.filter(LicensePool.work_id!=None)
        pools.update(update, synchronize_session='fetch')

        # Delete all work-genre assignments.
        genres = session.query(WorkGenre)
        if data_source:
            genres = genres.filter(WorkGenre.work_id.in_(work_ids_to_delete))
            print "Deleting %d genre assignments." % genres.count()
        genres.delete(synchronize_session='fetch')
        session.flush()

        works = session.query(Work)
        if data_source:
            print "Deleting %d works." % len(work_ids_to_delete)
            works = works.filter(Work.id.in_(work_ids_to_delete))
        works.delete(synchronize_session='fetch')
        session.commit()

    print "Creating new works."
    LicensePool.consolidate_works(session)
    session.commit()
