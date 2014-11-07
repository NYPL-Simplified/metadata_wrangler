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
        self.classifier = ClassifierFactory.from_file(
            self.training_dataset_path, self.classifier_path)

    def calculate_for_works(self, q, force=False):
        if not force:
            q = q.filter(Work.primary_appeal==None)
            for work in q:
                work.calculate_appeals(
                    self.amazon_api, self.classifier, self.feature_names)
                print (
                    work.title, work.appeal_character, work.appeal_language,
                    work.appeal_setting, work.appeal_story)
                self._db.commit()            

if __name__ == '__main__':
    data_directory = sys.argv[1]
    _db = production_session()
    calculator = AppealCalculator(_db, data_directory)
    for genre in ("Science Fiction", "Fantasy", "Mystery"):
        works = Work.with_genre(_db, genre)
        calculator.calculate_for_works(works)
