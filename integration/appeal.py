from nose.tools import set_trace
import csv
from csv import Dialect
from cStringIO import StringIO
from collections import Counter
from sklearn import svm

from text.blob import TextBlob

class WakaDialect(Dialect):
    delimiter=","
    doublequote = False
    escapechar = "\\"
    quotechar='"'
    lineterminator="\r\n"
    quoting=csv.QUOTE_NONNUMERIC


class ClassifierFactory(object):

    @classmethod
    def str_to_float(cls, x):
        try:
            if not x: return 0.0
            return float(x)
        except:
            print("[{x}] is not a float".format(x=x))
            return 0.0

    @classmethod
    def data_load(cls, path):
        column_labels = None
        data = []
        for line in open(path):
            v = line.strip().split(',')
            if column_labels is None:
                column_labels = v
            else:
                data.append(v)
        return column_labels, data

    @classmethod
    def data_load_parse(cls, path, label_position=1):
        column_labels, raw_data = cls.data_load(path)
        column_labels = column_labels[label_position+1:]
        data = []
        row_labels = []
        for row in raw_data:
            row_labels.append(row[label_position])
            data.append([cls.str_to_float(x) for x in row[label_position+1:]])
        return column_labels, row_labels, data

    @classmethod
    def train_classifier(cls, training_data, training_labels):
        clf = svm.SVC(kernel='poly')
        clf.fit(training_data, training_labels)
        return clf


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
