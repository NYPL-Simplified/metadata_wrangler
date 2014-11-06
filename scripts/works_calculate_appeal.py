"""Calculate the appeal for Work objects."""

import os
import site
import sys
from nose.tools import set_trace
d = os.path.split(__file__)[0]
site.addsitedir(os.path.join(d, ".."))

from integration.amazon import AmazonAPI
from integration.appeal import (
    ClassifierFactory,
    FeatureCounter,
)
from model import (
    DataSource,
    LicensePool,
    Identifier,
    Genre,
    Work,
    WorkGenre,
    Edition,
)
from model import production_session

class AppealCalculator(object):

    appeal_names = dict(language=Work.LANGUAGE_APPEAL,
                        character=Work.CHARACTER_APPEAL,
                        setting=Work.SETTING_APPEAL,
                        story=Work.STORY_APPEAL)

    def __init__(self, _db, data_directory):
        self._db = _db
        self.amazon_api = AmazonAPI(self._db)
        self.training_dataset_path = os.path.join(
            data_directory, "appeal", "training_dataset.csv")
        self.classifier_path = os.path.join(
            data_directory, "appeal", "classifier.pickle")
        self.feature_names = ClassifierFactory.feature_names(
            self.training_dataset_path)
        self.classifier = None

    def calculate_for_works(self, q, force=False):
        if not force:
            q = q.filter(Work.appeal==None)
            for work in q:
                work.appeal = self.calculate_for_work(work)
                print work.title, work.appeal
                self._db.commit()            

    def calculate_for_work(self, work):
        seen_reviews = set()
        counter = FeatureCounter(self.feature_names)
        ids = work.all_identifier_ids()
        identifiers = self._db.query(
            Identifier).filter(Identifier.type.in_(
                [Identifier.ISBN, Identifier.ASIN])).filter(
                    Identifier.id.in_(ids))
        for identifier in identifiers:
            for review_title, review in self.amazon_api.fetch_reviews(identifier):
                if review not in seen_reviews:
                    counter.add_counts(review_title)
                    counter.add_counts(review)
                    seen_reviews.add(review)
        if not self.classifier:
            self.classifier = ClassifierFactory.from_file(
            self.training_dataset_path, self.classifier_path)
        print " Found %s distinct reviews" % len(seen_reviews)
        if not seen_reviews:
            return Work.UNKNOWN_APPEAL
        prediction = self.classifier.predict(counter.row())[0]
        if prediction in self.appeal_names:
            prediction = self.appeal_names[prediction]
        return prediction

if __name__ == '__main__':
    data_directory = sys.argv[1]
    _db = production_session()
    calculator = AppealCalculator(_db, data_directory)
    for genre in ("Science Fiction", "Fantasy", "Mystery"):
        works = Work.with_genre(_db, genre)
        calculator.calculate_for_works(works)
