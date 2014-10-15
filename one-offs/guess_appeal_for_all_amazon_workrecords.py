from nose.tools import set_trace
import os
import site
import sys
import csv
d = os.path.split(__file__)[0]
site.addsitedir(os.path.join(d, ".."))

from integration.amazon import (
    AmazonScraper,
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

    def __init__(self, features_path, classifier_path, words_out, counts_out):
        self.features_path = features_path
        self.classifier_path = classifier_path
        self.features = ClassifierFactory.feature_names(features_path)
        self.classifier = None
        self.db = production_session()
        self.amazon = DataSource.lookup(self.db, DataSource.AMAZON)
        self.counter = FeatureCounter(self.features)
        self.api = AmazonScraper(self.db)

        self.seen = set()
        if os.path.exists(words_out):
            for row in csv.reader(open(words_out)):
                self.seen.add(row[0])
        print "Already seen %d" % len(self.seen)

        self.words_out = csv.writer(open(words_out, "a"), dialect=WakaDialect)
        self.counts_out = csv.writer(open(counts_out, "a"), dialect=WakaDialect)
        self.filter = AppealTextFilter()

    def run(self):
        q = self.db.query(Identifier).join(CoverageRecord).filter(CoverageRecord.data_source==self.amazon).filter(Identifier.type==Identifier.ASIN)
        for identifier in q:
            if identifier.identifier in self.seen:
                continue
            self.process(identifier)
            self.seen.add(identifier.identifier)

    def process(self, identifier):
        self.counter.clear()
        equivalents = identifier.equivalent_identifier_ids()
        editions = self.db.query(Edition).filter(Edition.primary_identifier_id.in_(equivalents))
        with_title = [x for x in editions if x.title]
        asin = identifier.identifier
        if not with_title:
            return
        edition = with_title[0]
        seen_review_titles = set()
        review_words = []
        for title, review in self.api.scrape_reviews(identifier):
            seen_review_titles.add(title)
            self.counter.add_counts(title)
            self.counter.add_counts(review)
            review_words.extend(self.filter.filter(title))
            review_words.extend(self.filter.filter(review))

        #if not self.classifier:
        #    self.classifier = ClassifierFactory.from_file(
        #        self.features_path, self.classifier_path)


        if not edition.author:
            edition.calculate_presentation()
        #url = "http://www.amazon.com/exec/obidos/ASIN/%s" % identifier.identifier
        #data = [url, edition.title, edition.author,
        #        str(len(seen_review_titles)),
        #        self.classifier.predict(self.counter.row())[0]]
        
        base = [asin, edition.title.encode("utf8"), edition.author.encode("utf8"), "",]
        words_data = base + [" ".join(review_words).encode("utf8")]
        features_data = base + self.counter.row()
        self.words_out.writerow(words_data)
        self.counts_out.writerow(features_data)
        print edition.title

    def fix(self, x):
        if isinstance(x, unicode):
            return x.encode("utf8")
        elif isinstance(x, str):
            return x
        else:
            return str(x)

f = "/Users/labs/appeal/training_dataset.csv"
c = "/Users/labs/appeal/classifier.pickle"
words = "/Users/labs/appeal/amazon_output_words.csv"
counts = "/Users/labs/appeal/amazon_output_counts.csv"
App(f, c, words, counts).run()
