from nose.tools import set_trace
import os
import site
import sys
d = os.path.split(__file__)[0]
site.addsitedir(os.path.join(d, ".."))

from textblob import TextBlob
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
predictions_output_file = "/Users/labs/Desktop/datamining/test set for popular Amazon books - predictions.csv"
words_output_file = "/Users/labs/Desktop/datamining/test set for popular Amazon books - words.csv"

seen = set()
if os.path.exists(predictions_output_file):
    for i in csv.reader(open(predictions_output_file)):
        seen.add(i[0])
    of = open(predictions_output_file, "a")
    predictions_out = csv.writer(of, dialect=csv.QUOTE_NONNUMERIC)
    of2 = open(words_output_file, "a")
    words_out = csv.writer(of2, dialect=csv.QUOTE_NONNUMERIC)
else:
    of = open(predictions_output_file, "w")
    predictions_out = csv.writer(of, dialect=csv.QUOTE_NONNUMERIC)
    predictions_out.writerow(["Identifier", "Title", "Author", "Prediction", "Training set input"])
    of2 = open(words_output_file, "a")
    words_out = csv.writer(of2, dialect=csv.QUOTE_NONNUMERIC)
    words_out.writerow(["Identifier", "Title", "Author", "Words"])

training_data = defaultdict(list)
for key, title, author, appeal, words in csv.reader(open(title_file), dialect=WakaDialect):
    training_data[title].append(appeal)

db = production_session()
features = ClassifierFactory.feature_names(f)

classifier = None
amazon = DataSource.lookup(db, DataSource.AMAZON)
scraper = AmazonScraper(db)
counter = FeatureCounter(features)
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
    titles = 0
    seen_reviews = set()
    sys.stderr.write("Working on %s\n" % edition.title)
    words = []
    blobwords = []
    for title, review in scraper.scrape_reviews(i):
        sys.stderr.write(" %s\n" % title)
        counter.add_counts(title)
        counter.add_counts(review)
        blobwords.append(title)
        blobwords.append(review)

    corpus = " ".join(blobwords)
    words = [x.lower() for x in TextBlob(corpus).words]

    if not classifier:
        sys.stderr.write("Loading\n")
        classifier = ClassifierFactory.from_file(f, c)
        sys.stderr.write("Done\n")

    a = [i.identifier, edition.title.encode("utf8"), authors.encode("utf8"), classifier.predict(counter.row(boolean=True))[0], ", ".join(training_data.get(edition.title, []))]
             
    predictions_out.writerow(a)
    of.flush()

    a = [i.identifier, edition.title.encode("utf8"), authors.encode("utf8"), " ".join(words).encode("utf8")]
    words_out.writerow(a)
    of2.flush()
