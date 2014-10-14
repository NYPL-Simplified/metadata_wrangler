from nose.tools import set_trace
import os
import site
import sys
d = os.path.split(__file__)[0]
site.addsitedir(os.path.join(d, ".."))

from integration.amazon import (
    AmazonScraper,
)

from integration.appeal import (
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

    def __init__(self, features_path, classifier_path):
        self.features_path = features_path
        self.classifier_path = classifier_path
        self.features = ClassifierFactory.feature_names(features_path)
        self.classifier = None
        self.db = production_session()
        self.amazon = DataSource.lookup(self.db, DataSource.AMAZON)
        self.counter = FeatureCounter(self.features)
        self.api = AmazonScraper(self.db)

    def run(self):
        q = self.db.query(Identifier).join(CoverageRecord).filter(CoverageRecord.data_source==self.amazon).filter(Identifier.type==Identifier.ASIN).filter(Identifier.identifier=="B00H7MBJYK")
        for identifier in q:
            self.process(identifier)

    def process(self, identifier):

        self.counter.clear()
        equivalents = identifier.equivalent_identifier_ids()
        editions = self.db.query(Edition).filter(Edition.primary_identifier_id.in_(equivalents))
        with_title = [x for x in editions if x.title]
        if not with_title:
            return
        edition = with_title[0]
        seen_review_titles = set()
        set_trace()
        for title, review in self.api.scrape_reviews(identifier):
            if title in seen_review_titles:
                continue
            seen_review_titles.add(title)
            self.counter.add_counts(title)
            self.counter.add_counts(review)

        if not self.classifier:
            self.classifier = ClassifierFactory.from_file(
                self.features_path, self.classifier_path)


        if not edition.author:
            edition.calculate_presentation()
        url = "http://www.amazon.com/exec/obidos/ASIN/%s" % identifier.identifier
        data = [url, edition.title, edition.author,
                str(len(seen_review_titles)),
                self.classifier.predict(self.counter.row())[0]]
        print "\t".join([self.fix(x) for x in data])

    def fix(self, x):
        if isinstance(x, unicode):
            return x.encode("utf8")
        elif isinstance(x, str):
            return x
        else:
            return str(x)

f = "/Users/labs/appeal/training_dataset.csv"
c = "/Users/labs/appeal/classifier.pickle"
App(f, c).run()    
