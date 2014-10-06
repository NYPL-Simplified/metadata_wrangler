import csv
from csv import Dialect
from cStringIO import StringIO
from collections import Counter

from text.blob import TextBlob

class WakaDialect(Dialect):
    delimiter=","
    doublequote = False
    escapechar = "\\"
    quotechar='"'
    lineterminator="\r\n"
    quoting=csv.QUOTE_NONNUMERIC


class FeatureCounter(Counter):

    def __init__(self, features):
        self.features = []
        for i in features:
            key = i.split(" ")
            if len(key) > 2:
                raise ValueError(
                    'Feature %r has more than two words, which is not supported.' % key
                )
            elif len(key) == 1:
                key = key[0]
            else:
                key = tuple(key)
            self.features.append(key)
        self.feature_set = set(self.features)

    def add_counts(self, text):
        last_word = None
        for word in TextBlob(text).words:
            word = word.lower()
            if word in self.feature_set:
                self[word] += 1
            if last_word and (last_word, word) in self.feature_set:
                self[(last_word, word)] += 1
            last_word = word

    def row(self):
        return [self[i] for i in self.features]
