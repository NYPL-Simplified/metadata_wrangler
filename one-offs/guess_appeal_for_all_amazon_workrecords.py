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
    Identifier,
)

f = "/home/leonardr/data/appeal/training_dataset.csv"
c = "/home/leonardr/data/appeal/classifier.pickle"
db = production_session()
CoverageRecord

identifier, ignore = Identifier.for_foreign_id(db, Identifier.ASIN, "B004J4WKUQ")
features = ClassifierFactory.feature_names(f)
classifier = None

amazon = DataSource.lookup(db, DataSource.AMAZON)
scraper = AmazonScraper(db)
for i in db.query(WorkIdentifier).join(CoverageRecord).filter(CoverageRecord.data_source==amazon):
    counter = FeatureCounter(features)
    print i
    titles = 0
    seen_titles = set()
    for title, review in scraper.scrape_reviews(identifier):
        if title in seen_titles:
            continue
        seen_titles.add(title)
        print title
        counter.add_counts(title)
        counter.add_counts(review)
    print titles
    if not classifier:
        classifier = ClassifierFactory.from_file(f, c)
    classifier.predict(counter.row())
    
