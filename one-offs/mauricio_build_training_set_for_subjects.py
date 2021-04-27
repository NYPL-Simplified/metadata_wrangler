# encoding:utf-8
from pdb import set_trace
import sys
import os
import numpy
import csv
from csv import Dialect
from textblob import TextBlob
from collections import defaultdict

import site
d = os.path.split(__file__)[0]
site.addsitedir(os.path.join(d, ".."))

from model import (
    Classification,
    LicensePool,
    DataSource,
    WorkIdentifier,
    production_session
)

class WakaDialect(Dialect):
    delimiter=","
    doublequote = False
    escapechar = "\\"
    quotechar='"'
    lineterminator="\r\n"
    quoting=csv.QUOTE_NONNUMERIC

class SubjectFinder(object):

    def __init__(self, db):
        self._db = db
        self.overdrive = DataSource.lookup(self._db, DataSource.OVERDRIVE)

    def write(self, output_file):
        out = csv.writer(open(output_file, "w"), 
                         dialect=WakaDialect)
        all_genres = set()

        c = 0
        for lp in self._db.query(LicensePool).filter(LicensePool.data_source==self.overdrive):
            work = lp.work
            if not work:
                continue
            if not work.genres:
                continue
            ids = lp.identifier.equivalent_identifier_ids()
            subjects = set([x.subject for x in self._db.query(Classification).filter(Classification.work_identifier_id.in_(ids)).filter(Classification.data_source != self.overdrive)])
            if not subjects:
                continue
            all_subjects = []
            for s in subjects:
                r = "|".join([s.type or "", s.identifier or "", s.name or ""])
                r = r.replace(";", ",")
                all_subjects.append(r)
            for genre in work.genres:
                all_genres.add('"%s"' % genre.name)
                out.writerow(
                    [lp.identifier.identifier, work.title.encode("utf8"),
                     work.fiction, work.audience, genre.name,
                     (";".join(all_subjects)).encode("utf8")])
            c += 1
            if not c % 100:
                print(c)

        print("{%s}" % (",".join(sorted(all_genres))))

if __name__ == '__main__':
    db = production_session()
    data_dir = sys.argv[1]
    output_path = os.path.join(data_dir, "Interest Vocabulary", "training set with subjects.csv")
    SubjectFinder(db).write(output_path)
