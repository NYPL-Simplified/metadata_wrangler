from pdb import set_trace
import os
import site
import sys

d = os.path.split(__file__)[0]
site.addsitedir(os.path.join(d, ".."))

from model import (
    SessionManager,
    Work,
)

from model import production_session
db = production_session()

def explain_identifier(identifier):
    print "  %r" % identifier

def explain_workrecord(wr):
    authors = [x.get('name', '') for x in wr.authors]
    foo = ' Record %d: "%s" by %s' % (wr.id, wr.title, authors)
    print foo.encode("utf8")
    for identifier in wr.equivalent_identifiers:
        explain_identifier(identifier)

def explain_licensepool(lp):
    wr = lp.work_record(db)
    print " %s/%s %r" % (
        wr.primary_identifier.type, wr.primary_identifier.identifier,
        wr
    )

def explain_work(work):
    work.calculate_presentation(db)
    foo = 'Work %d: "%s" by %s' % (work.id, work.title, work.authors)
    print foo.encode("utf8")
    print 'License pools:'
    for pool in work.license_pools:
        explain_licensepool(pool)
    print
    print 'Directly related work records:'
    for record in work.work_records:
        explain_workrecord(record)

    print
    print 'Indirectly related work records:'
    for record in work.all_workrecords(db):
        explain_workrecord(record)
        for identifier in record.equivalent_identifiers:
            explain_identifier(identifier)


if __name__ == '__main__':

    worksq = db.query(Work)
    work_id = sys.argv[1]
    try:
        work_id = int(work_id)
        worksq = worksq.filter(Work.id==work_id)
    except ValueError, e:
        worksq = worksq.filter(Work.title==work_id)

    for work in worksq.order_by(Work.id):
        explain_work(work)
        print
        print "-" * 80
        print
