from nose.tools import set_trace
import os
import site
import sys
import csv
import string
import isbnlib
d = os.path.split(__file__)[0]
site.addsitedir(os.path.join(d, ".."))

from integration.amazon import (
    AmazonAPI,
)

from integration.appeal import (
    WakaDialect,
    AppealTextFilter,
    ClassifierFactory,
    FeatureCounter,
)
from model import (
    production_session,
    CoverageRecord,
    DataSource,
    Edition,
    Identifier,
)

class App:

    def __init__(self, classifications_path, training_out_path, failures_out_path):
        self.classifications_path = classifications_path
        self.training_out_path = training_out_path
        self.db = production_session()
        self.amazon = DataSource.lookup(self.db, DataSource.AMAZON)
        self.api = AmazonAPI(self.db)

        self.seen = set()
        if os.path.exists(training_out_path):
            for row in csv.reader(open(training_out_path), dialect=WakaDialect):
                self.seen.add(row[0])
        print "Already seen %d" % len(self.seen)

        self.training_out = csv.writer(open(training_out_path, "a"), dialect=WakaDialect)
        self.training_out.writerow(["Key", "Title", "Author", "Primary Appeal", "Words"])
        self.failures_out = csv.writer(open(failures_out_path, "w"), dialect=WakaDialect)
        self.filter = AppealTextFilter()
        self.classifications_in = csv.reader(open(classifications_path),
                                             dialect=csv.excel_tab)
        self.classifications_in.next()

    def run(self):
        for row in self.classifications_in:
            self.process_row(row)

    def find_asin_dammit(self, title):
        editions = [x for x in self.db.query(Edition).filter(Edition.title.ilike(title))]
        for edition in editions:
            identifiers = edition.equivalent_identifiers().all()
            for i in identifiers:
                if i.type == Identifier.ASIN:
                    return i.identifier
        return None

    def process_row(self, row):
        source, title, author, asin, primary_appeal, ignore, character, language, setting, story = row
        if not asin:
            asin = self.find_asin_dammit(title)

        if asin:
            asin = string.zfill(asin, 10)
        else:
            self.failures_out.writerow(row)
            return

        key = source + "-" + asin

        if key in self.seen:
            return

        if isbnlib.is_isbn10(asin):
            type = Identifier.ISBN
        else:
            type = Identifier.ASIN

        identifier, is_new = Identifier.for_foreign_id(self.db, type, asin)
        review_words = []
        print identifier.identifier, title
        for review_title, review in self.api.fetch_reviews(identifier):
            review_words.extend(self.filter.filter(review_title))
            review_words.extend(self.filter.filter(review))

        row = [key, title, author, primary_appeal, " ".join(review_words).encode("utf8")]
        self.db.commit()
        self.training_out.writerow(row)
        

    def fix(self, x):
        if isinstance(x, unicode):
            return x.encode("utf8")
        elif isinstance(x, str):
            return x
        else:
            return str(x)

if __name__ == '__main__':
    classifications, words, failures = sys.argv[1:]
    App(classifications, words, failures).run()
