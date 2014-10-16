from nose.tools import set_trace
import os
import site
import sys
d = os.path.split(__file__)[0]
site.addsitedir(os.path.join(d, ".."))

from integration.amazon import (
    AmazonScraper,
)
import csv
from collections import defaultdict

from integration.appeal import (
    ClassifierFactory,
    FeatureCounter,
    WakaDialect
)
from model import (
    DataSource,
    CoverageRecord,
    production_session,
    Identifier,
    Measurement,
)

f = "/Users/labs/simplified/data/appeal/training_dataset.csv"
c = "/Users/labs/simplified/data/appeal/classifier.pickle"
title_file = "/Users/labs/Desktop/datamining/master appeal training set words.csv"
output_file = "/Users/labs/Desktop/datamining/predictions on popular Amazon books.tsv"
feature_file = "/Users/labs/Desktop/datamining/features for popular Amazon books.tsv"

seen = set()
feature_names = ClassifierFactory.feature_names(f)
if os.path.exists(output_file):
    for i in csv.reader(open(output_file)):
        seen.add(i[0])
    of = open(output_file, "a")
    of2 = open(feature_file, "a")
    out = csv.writer(of, dialect=csv.QUOTE_NONNUMERIC)
    feature_out = csv.writer(of2, dialect=csv.QUOTE_NONNUMERIC)
else:
    of = open(output_file, "w")
    out = csv.writer(of, dialect=csv.QUOTE_NONNUMERIC)
    out.writerow(["Identifier", "Title", "Author", "Number of Reviews", "Prediction", "Training set input"])
    of2 = open(feature_file, "a")
    feature_out = csv.writer(of2, dialect=csv.QUOTE_NONNUMERIC)
    feature_out.writerow(["Identifier", "Title", "Author", "Number of Reviews"] + feature_names)

training_data = defaultdict(list)
for key, title, author, appeal, words in csv.reader(open(title_file), dialect=WakaDialect):
    training_data[title].append(appeal)

db = production_session()

classifier = None
amazon = DataSource.lookup(db, DataSource.AMAZON)
scraper = AmazonScraper(db)
counter = FeatureCounter(feature_names)
q = db.query(Identifier).join(Measurement).filter(Measurement.data_source==amazon).filter(Measurement.quantity_measured==Measurement.POPULARITY).filter(Measurement.value < 20000)
for i in q:
    if i.identifier in seen:
        print "Skipping %s" % i.identifier
        continue
    ids = i.equivalent_identifier_ids()
    edition = None
    for equiv in db.query(Identifier).filter(Identifier.id.in_(ids)):
        if equiv.primarily_identifies:
            edition = equiv.primarily_identifies[0]
            break
    if not edition:
        continue
    counter.clear()
    authors = " ; ".join([x.name for x in edition.author_contributors])
    num_reviews = 0
    sys.stderr.write("Working on %s\n" % edition.title)
    for title, review in scraper.scrape_reviews(i):
        num_reviews += 1
        sys.stderr.write(" %s\n" % title)
        counter.add_counts(title)
        counter.add_counts(review)
    db.commit()

    if not classifier:
        sys.stderr.write("Loading\n")
        classifier = ClassifierFactory.from_file(f, c)
        sys.stderr.write("Done\n")

    guess = [i.identifier, edition.title.encode("utf8"), authors.encode("utf8"), num_reviews, classifier.predict(counter.row())[0], ", ".join(training_data.get(edition.title, []))]
    features = [i.identifier, edition.title.encode("utf8"), authors.encode("utf8"), num_reviews] + counter.row()

    out.writerow(guess)
    of.flush()
    feature_out.writerow(features)
    of2.flush()

