import os
import site
import sys
from nose.tools import set_trace
d = os.path.split(__file__)[0]
site.addsitedir(os.path.join(d, ".."))
from integration.fast import FASTNames
from model import (
    production_session,
    Subject,
)

if __name__ == '__main__':

    data_directory = sys.argv[1]
    names = FASTNames.from_data_directory(data_directory)
    print "Loaded %s names." % len(names)
    _db = production_session()
    q = _db.query(Subject).filter(Subject.type==Subject.FAST).filter(
        Subject.name==None)
    q = _db.query(Subject).filter(Subject.type==Subject.FAST)
    print "Considering %d Subjects." % q.count()
    c = 0
    for subject in q:
        if subject.identifier in names:
            new_name = names[subject.identifier]
            if subject.name and new_name != subject.name:
                f = "%s\t%s\t%s" % (subject.identifier, subject.name, new_name)
                print f.encode("utf8")
            subject.name = new_name
            c += 1
            if not c % 1000:
                _db.commit()
        else:
            print "MISSING %s" % subject.identifier
    _db.commit()
