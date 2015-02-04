import cPickle
from nose.tools import set_trace
import os
import csv
from csv import Dialect
from cStringIO import StringIO
from collections import Counter

from textblob import TextBlob

from core.model import (
    Identifier,
    Session,
    Work,
)
from amazon import AmazonAPI

class WakaDialect(Dialect):
    delimiter=","
    doublequote = False
    escapechar = "\\"
    quotechar='"'
    lineterminator="\r\n"
    quoting=csv.QUOTE_NONNUMERIC


class AppealTextFilter(object):
    """Filter review text to just adjectives, adverbs, and selected nouns
    and noun phrases.
    """
    STOPWORDS = set(["a's", "able", "about", "above", "according",
                 "accordingly", "across", "actually", "after", "afterwards",
                 "again", "against", "ain't", "all", "allow",
                 "allows", "almost", "alone", "along", "already",
                 "also", "although", "always", "am", "among",
                 "amongst", "an", "and", "another", "any",
                 "anybody", "anyhow", "anyone", "anything", "anyway",
                 "anyways", "anywhere", "apart", "appear", "appreciate",
                 "appropriate", "are", "aren't", "around", "as",
                 "aside", "ask", "asking", "associated", "at",
                 "available", "away", "awfully", "be", "became",
                 "because", "become", "becomes", "becoming", "been",
                 "before", "beforehand", "behind", "being", "believe",
                 "below", "beside", "besides", "best", "better",
                 "between", "beyond", "both", "brief", "but",
                 "by", "c'mon", "c's", "came", "can",
                 "can't", "cannot", "cant", "cause", "causes",
                 "certain", "certainly", "changes", "clearly", "co",
                 "com", "come", "comes", "concerning", "consequently",
                 "consider", "considering", "contain", "containing", "contains",
                 "corresponding", "could", "couldn't", "course", "currently",
                 "definitely", "described", "despite", "did", "didn't",
                 "different", "do", "does", "doesn't", "doing",
                 "don't", "done", "down", "downwards", "during",
                 "each", "edu", "eg", "eight", "either",
                 "else", "elsewhere", "enough", "entirely", "especially",
                 "et", "etc", "even", "ever", "every",
                 "everybody", "everyone", "everything", "everywhere", "ex",
                 "exactly", "example", "except", "far", "few",
                 "fifth", "first", "five", "followed", "following",
                 "follows", "for", "former", "formerly", "forth",
                 "four", "from", "further", "furthermore", "get",
                 "gets", "getting", "given", "gives", "go",
                 "goes", "going", "gone", "got", "gotten",
                 "greetings", "had", "hadn't", "happens", "hardly",
                 "has", "hasn't", "have", "haven't", "having",
                 "he", "he's", "hello", "help", "hence",
                 "her", "here", "here's", "hereafter", "hereby",
                 "herein", "hereupon", "hers", "herself", "hi",
                 "him", "himself", "his", "hither", "hopefully",
                 "how", "howbeit", "however", "i'd", "i'll",
                 "i'm", "i've", "ie", "if", "ignored",
                 "immediate", "in", "inasmuch", "inc", "indeed",
                 "indicate", "indicated", "indicates", "inner", "insofar",
                 "instead", "into", "inward", "is", "isn't",
                 "it", "it'd", "it'll", "it's", "its",
                 "itself", "just", "keep", "keeps", "kept",
                 "know", "known", "knows", "last", "lately",
                 "later", "latter", "latterly", "least", "less",
                 "lest", "let", "let's", "like", "liked",
                 "likely", "little", "look", "looking", "looks",
                 "ltd", "mainly", "many", "may", "maybe",
                 "me", "mean", "meanwhile", "merely", "might",
                 "more", "moreover", "most", "mostly", "much",
                 "must", "my", "myself", "name", "namely",
                 "nd", "near", "nearly", "necessary", "need",
                 "needs", "neither", "never", "nevertheless", "new",
                 "next", "nine", "no", "nobody", "non",
                 "none", "noone", "nor", "normally", "not",
                 "nothing", "novel", "now", "nowhere", "obviously",
                 "of", "off", "often", "oh", "ok",
                 "okay", "old", "on", "once", "one",
                 "ones", "only", "onto", "or", "other",
                 "others", "otherwise", "ought", "our", "ours",
                 "ourselves", "out", "outside", "over", "overall",
                 "own", "particular", "particularly", "per", "perhaps",
                 "placed", "please", "plus", "possible", "presumably",
                 "probably", "provides", "que", "quite", "qv",
                 "rather", "rd", "re", "really", "reasonably",
                 "regarding", "regardless", "regards", "relatively", "respectively",
                 "right", "said", "same", "saw", "say",
                 "saying", "says", "second", "secondly", "see",
                 "seeing", "seem", "seemed", "seeming", "seems",
                 "seen", "self", "selves", "sensible", "sent",
                 "serious", "seriously", "seven", "several", "shall",
                 "she", "should", "shouldn't", "since", "six",
                 "so", "some", "somebody", "somehow", "someone",
                 "something", "sometime", "sometimes", "somewhat", "somewhere",
                 "soon", "sorry", "specified", "specify", "specifying",
                 "still", "sub", "such", "sup", "sure",
                 "t's", "take", "taken", "tell", "tends",
                 "th", "than", "thank", "thanks", "thanx",
                 "that", "that's", "thats", "the", "their",
                 "theirs", "them", "themselves", "then", "thence",
                 "there", "there's", "thereafter", "thereby", "therefore",
                 "therein", "theres", "thereupon", "these", "they",
                 "they'd", "they'll", "they're", "they've", "think",
                 "third", "this", "thorough", "thoroughly", "those",
                 "though", "three", "through", "throughout", "thru",
                 "thus", "to", "together", "too", "took",
                 "toward", "towards", "tried", "tries", "truly",
                 "try", "trying", "twice", "two", "un",
                 "under", "unfortunately", "unless", "unlikely", "until",
                 "unto", "up", "upon", "us", "use",
                 "used", "useful", "uses", "using", "usually",
                 "value", "various", "very", "via", "viz",
                 "vs", "want", "wants", "was", "wasn't",
                 "way", "we", "we'd", "we'll", "we're",
                 "we've", "welcome", "well", "went", "were",
                 "weren't", "what", "what's", "whatever", "when",
                 "whence", "whenever", "where", "where's", "whereafter",
                 "whereas", "whereby", "wherein", "whereupon", "wherever",
                 "whether", "which", "while", "whither", "who",
                 "who's", "whoever", "whole", "whom", "whose",
                 "why", "will", "willing", "wish", "with",
                 "within", "without", "won't", "wonder", "would",
                 "wouldn't", "yes", "yet", "you", "you'd",
                 "you'll", "you're", "you've", "your", "yours",
                 "yourself", "yourselves", "zero",

                    "a", "able", "about", "across", "after", "all", "almost", "also", "am", "among", "an", "and", "any", "are", "as", "at", "be", "because", "been", "but", "by", "can", "cannot", "could", "dear", "did", "do", "does", "either", "else", "ever", "every", "for", "from", "get", "got", "had", "has", "have", "he", "her", "hers", "him", "his", "how", "however", "i", "if", "in", "into", "is", "it", "its", "just", "least", "let", "like", "likely", "may", "me", "might", "most", "must", "my", "neither", "no", "nor", "not", "of", "off", "often", "on", "only", "or", "other", "our", "own", "rather", "said", "say", "says", "she", "should", "since", "so", "some", "than", "that", "the", "their", "them", "then", "there", "these", "they", "this", "tis", "to", "too", "twas", "us", "wants", "was", "we", "were", "what", "when", "where", "which", "while", "who", "whom", "why", "will", "with", "would", "yet", "you", "your", "ain't", "aren't", "can't", "could've", "couldn't", "didn't", "doesn't", "don't", "hasn't", "he'd", "he'll", "he's", "how'd", "how'll", "how's", "i'd", "i'll", "i'm", "i've", "isn't", "it's", "might've", "mightn't", "must've", "mustn't", "shan't", "she'd", "she'll", "she's", "should've", "shouldn't", "that'll", "that's", "there's", "they'd", "they'll", "they're", "they've", "wasn't", "we'd", "we'll", "we're", "weren't", "what'd", "what's", "when'd", "when'll", "when's", "where'd", "where'll", "where's", "who'd", "who'll", "who's", "why'd", "why'll", "why's", "won't", "would've", "wouldn't", "you'd", "you'll", "you're", "you've", "'s", "n't", "'m", "'d", "'", "*",


                    "read", "reading", "kindle", "amazon", "book", "books",

                     'more', 'most', 'less', 'least', 'not', 'so',
                     'very', 'also', 'then', "just", "much",
                     "only", "other", "never", "many",
                     "even", "yet", "too", "really",
                     "such", "first", "often",
                     "back", "whole", "else",

                     "--",

                 ])


    WHITELIST = set([])

    ADJACENT_TAG_BLACKLIST = set(['DT', 'PRP', 'PRP$', 'CC'])

    ADJACENT_WORDS = set(['setting', 'worldbuilding', 'world-building',
                          'character', 'characters', 'characterization', 
                          'language', 'imagery', 'writing', 'prose',
                          'narrative', 'narratives', 'written',
                          'story', 'stories', 'storyteller', 'storytelling',
                          'tale', 'tales',
                          'plot', 'plotted', 'plotting', 'subplot',
                          'twist', 'twists',
    ])

    TAGS = set(["RB", "RBR", "RBS", "JJ", "JJR", "JJS"])

    def filter(self, text):
        filtered = []
        previous_word = None
        if not text:
            return filtered
        tags = TextBlob(text).tags
        for i, (word, tag) in enumerate(tags):
            word = word.lower()
            if word in self.STOPWORDS or tag in self.ADJACENT_TAG_BLACKLIST:
                previous_word = None
                continue

            if word in self.ADJACENT_WORDS:
                if i < len(tags)-1:
                    next_word, next_tag = tags[i+1]
                    next_word = next_word.lower()
                    if (next_word not in self.STOPWORDS
                        and next_tag not in self.ADJACENT_TAG_BLACKLIST):
                        filtered.append(word + "-" + next_word)
                if previous_word:
                    filtered.append(previous_word + "-" + word)
            if tag in self.TAGS or word in self.WHITELIST:
                filtered.append(word)
            previous_word = word
        return filtered


class ClassifierFactory(object):

    @classmethod
    def from_file(cls, path, cache_at):
        if os.path.exists(cache_at):
            with open(cache_at, 'rb') as fid:
                classifier = cPickle.load(fid)
        else:
            data, labels = cls.parse_data_and_labels(path)
            classifier = cls.from_data_and_labels(data, labels)
            with open(cache_at, 'wb') as fid:
                cPickle.dump(classifier, fid)
        return classifier

    @classmethod
    def feature_names(self, path):
        # Element 0 is 'identifier', element 1 is 'primary appeal'.
        reader = csv.reader(open(path))
        return reader.next()[2:]

    @classmethod
    def parse_data_and_labels(cls, path):
        labels = []
        data = []

        reader = csv.reader(open(path))
        # Ignore the first row--it's the feature names.
        reader.next()

        # Element 0 of the row is an identifier, which we ignore.
        # Element 1 is the classification, which we treat as a label.
        # Subsequent elements are features.
        for row in reader:
            labels.append(row[1])
            row_data = []
            data.append(row_data)
            for x in row[2:]:
                row_data.append(cls.str_to_float(x))
        return data, labels
        
    @classmethod
    def from_data_and_labels(cls, training_data, training_labels):
        from sklearn import linear_model
        clf = linear_model.LogisticRegression()
        clf.fit(training_data, training_labels)
        return clf

    @classmethod
    def str_to_float(cls, x):
        if x=='': return 0.0
        if x=='false': return 0.0
        if x=='true': return 1.0
        try:
            return float(x)
        except:
            print("[{x}] is not a float".format(x=x))
            return 0.0


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

    blacklist = set([
        'bad',
        'big',
        'but',
        'excellent',
        'extremely',
        'favorite',
        'finally', 
        'full',
        'good',
        'great',
        'hard',
        'human',
        'interesting',
        'long',
        'main',
        'past',
        'present',
        'short',
        'teenage',
        'young',
        'with',
        'book',
        'out',
        'read',
        'who',
        'about',
        'one',
        'just',
        'all',
        'like',
        'get',
        'what',
        'very',
    ])

    def add_counts(self, text):
        if not text:
            return
        last_word = None
        for word in TextBlob(text).words:
            word = word.lower()
            if word in self.blacklist:
                continue
            if word in self.feature_set:
                self[word] += 1
            if last_word and (last_word, word) in self.feature_set:
                self[(last_word, word)] += 1
            last_word = word

    def row(self, boolean=False):
        if boolean:
            x = []
            for i in self.features:
                if self[i] > 0:
                    x.append(1)
                else:
                    x.append(0)
            return x
        else:
            return [self[i] for i in self.features]

    def calculate_appeals_for_work(self, work, review_source, classifier):
        _db = Session.object_session(work)
        seen_reviews = set()
        ids = work.all_identifier_ids()
        identifiers = _db.query(Identifier).filter(
            Identifier.type.in_([Identifier.ISBN, Identifier.ASIN])).filter(
                Identifier.id.in_(ids))
        a = 0
        for identifier in identifiers:
            for review_title, review in review_source.fetch_reviews(identifier):
                if review not in seen_reviews:
                    self.add_counts(review_title)
                    self.add_counts(review)
                    seen_reviews.add(review)
                a += 1
                if a > 100:
                    break
            if a > 100:
                break
        print " Found %s distinct reviews" % len(seen_reviews)
        if seen_reviews:
            appeals = classifier.predict_proba(self.row())[0]
        else:
            appeals = [0,0,0,0]
        work.assign_appeals(*appeals)


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
        self.feature_counter = FeatureCounter(self.feature_names)

    def calculate_for_work(self, work):
        print "BEFORE pri=%s sec=%s cha=%.3f lan=%.3f set=%.3f sto=%.3f %s %s" % (
            work.primary_appeal, work.secondary_appeal,
            work.appeal_character or 0, work.appeal_language or 0,
            work.appeal_setting or 0, work.appeal_story or 0, work.title, work.author)
        old_language = work.appeal_language
        old_setting = work.appeal_setting

        self.feature_counter.calculate_appeals_for_work(
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
