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
        self.classifier = ClassifierFactory.from_file(
            self.training_dataset_path, self.classifier_path)

    def calculate_for_works(self, q, force=False):
        if not force:
            q = q.filter(Work.primary_appeal==None)
        feature_counter = FeatureCounter(self.feature_names)
        for work in q:
            print "BEFORE pri=%s sec=%s cha=%.3f lan=%.3f set=%.3f sto=%.3f %s %s" % (
                work.primary_appeal, work.secondary_appeal,
                work.appeal_character or 0, work.appeal_language or 0,
                work.appeal_setting or 0, work.appeal_story or 0, work.title, work.author)
            old_language = work.appeal_language
            old_setting = work.appeal_setting

            feature_counter.calculate_appeals_for_work(
                work, self.amazon_api, self.classifier)
            print "AFTER pri=%s sec=%s cha=%.3f lan=%.3f set=%.3f sto=%.3f %s %s" % (
                work.primary_appeal, work.secondary_appeal,
                work.appeal_character, work.appeal_language,
                work.appeal_setting, work.appeal_story, work.title, work.author)
            if old_language:
                print "LANGUAGE DELTA: %.7f" % (old_language - work.appeal_language)
            if old_setting:
                print "SETTING DELTA: %.7f" % (old_setting - work.appeal_setting)

            print ""
            self._db.commit()

if __name__ == '__main__':
    data_directory = sys.argv[1]
    _db = production_session()
    calculator = AppealCalculator(_db, data_directory)
    works = _db.query(Work).filter(Work.primary_appeal.in_([Work.LANGUAGE_APPEAL, Work.SETTING_APPEAL, Work.STORY_APPEAL, Work.SETTING_APPEAL]))
    #works = Work.with_genre(_db, "Historical Romance")
    works = _db.query(Work).filter(Work.fiction==True)
    print works.count()
    calculator.calculate_for_works(works, force=True)
