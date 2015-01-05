"""Calculate the appeal for Work objects."""

import os
import site
import sys
from nose.tools import set_trace
d = os.path.split(__file__)[0]
site.addsitedir(os.path.join(d, ".."))

from integration.amazon import AmazonAPI
from integration.appeal import (
    AppealCalculator
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

if __name__ == '__main__':
    data_directory = sys.argv[1]
    _db = production_session()
    calculator = AppealCalculator(_db, data_directory)
    works = _db.query(Work).filter(Work.primary_appeal.in_([Work.LANGUAGE_APPEAL, Work.SETTING_APPEAL, Work.STORY_APPEAL, Work.SETTING_APPEAL]))
    #works = Work.with_genre(_db, "Historical Romance")
    works = _db.query(Work).filter(Work.fiction==True)
    print works.count()
    calculator.calculate_for_works(works, force=True)
