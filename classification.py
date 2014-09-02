import json
import pkgutil
from collections import (
    Counter,
    defaultdict,
)
from nose.tools import set_trace
import re

from util import MetadataSimilarity

import lane

class AssignSubjectsToLanes(object):

    def __init__(self, _db):
        self._db = _db

    def run(self, force=False):
        from model import Subject
        q = self._db.query(Subject).filter(Subject.locked==False)
        if not force:
            q = q.filter(Subject.lane==None)
        for subject in q:
            classifier = Classification.classifiers.get(
                subject.type, None)
            if not classifier:
                continue
            lane, audience, fiction = classifier.classify(subject)
            print subject, lane, audience, fiction
            set_trace()
            if lane:
                subject.lane = lane
            if audience:
                subject.audience = audience
            if fiction:
                subject.fiction = fiction


class Classification(object):

    AUDIENCE_CHILDREN = "Children"
    AUDIENCE_YOUNG_ADULT = "Young Adult"
    AUDIENCE_ADULT = "Adult"

    classifiers = dict()

    @classmethod
    def name_for(cls, identifier):
        """Look up a human-readable name for the given identifier."""
        return None

    @classmethod
    def classify(cls, subject):
        """Try to determine lane, audience, and fiction status
        for the given Subject.
        """
        identifier = cls.scrub_identifier(subject.identifier)
        if subject.name:
            name = cls.scrub_name(subject.name)
        else:
            name = identifier
        return (cls.lane(identifier, name),
                cls.audience(identifier, name),
                cls.is_fiction(identifier, name))

    @classmethod
    def scrub_identifier(cls, identifier):
        """Prepare an identifier from within a call to classify().
        
        This may involve data normalization, conversion to lowercase,
        etc.
        """
        return identifier.lower()

    @classmethod
    def scrub_name(cls, name):
        """Prepare a name from within a call to classify()."""
        return name.lower()

    @classmethod
    def lane(cls, identifier, name):
        """Is this identifier associated with a particular Lane?"""
        return None

    @classmethod
    def is_fiction(cls, identifier, name):
        """Is this identifier+name particularly indicative of fiction?
        How about nonfiction?
        """
        n = name.lower()
        if "nonfiction" in n:
            return False
        if "fiction" in n:
            return True
        return None

    @classmethod
    def audience(cls, identifier, name):
        """What does this identifier+name say about the audience for
        this book?
        """
        n = name.lower()
        if 'juvenile' in n:
            return cls.AUDIENCE_CHILDREN
        elif 'young adult' in n or "YA" in name:
            return cls.AUDIENCE_YOUNG_ADULT
        return None


class OverdriveClassification(Classification):

    FICTION = set([
        "Juvenile Fiction",
        "Young Adult Fiction",
        ])

    JUVENILE = set([
        "Juvenile Fiction",
        ])

    YOUNG_ADULT = set([
        "Young Adult Fiction",
        ])

    LANES = {
        lane.History : set(["History"]),
        lane.Biography : set(["Biography & Autobiography"]),
        lane.Romance : set(["Romance"]),
        lane.Mystery : set(["Mystery"]),
    }

    @classmethod
    def scrub_identifier(cls, identifier):
        return identifier

    def fiction(cls, identifier):
        return identifier in cls.FICTION

    def audience(cls, identifier):
        if identifier in cls.JUVENILE:
            return cls.AUDIENCE_CHILDREN
        elif identifier in cls.YOUNG_ADULT:
            return cls.AUDIENCE_YOUNG_ADULT
        return None

    def lane(cls, identifier):
        for l, v in cls.LANES.items():
            if identifier in v:
                return l
        return None


class DeweyDecimalClassification(Classification):

    NAMES = None
    FICTION = set([800, 810, 811, 812, 813, 817, 820, 821, 822, 823, 827])

    LANES = {
        lane.History : set(
            range(930, 941) + [900, 904, 909, 950, 960, 970, 980, 990]
        ),
        lane.Biography : set(
            [920, "B"]
        ),
        lane.Philosophy : set(
            range(140, 150) + range(180, 201) + [100, 101]
        ),
        lane.Religion : set(
            range(200,300)
        ),
    }

    @classmethod
    def _load(cls):
        cls.NAMES = json.loads(
            pkgutil.get_data("resources", "dewey_1000.json"))

        # Add some other values commonly found in MARC records.
        cls.DEWEY["B"] = "Biography"
        cls.DEWEY["E"] = "Juvenile Fiction"
        cls.DEWEY["F"] = "Fiction"
        cls.DEWEY["FIC"] = "Juvenile Fiction"
        cls.DEWEY["J"] = "Juvenile Nonfiction"
        cls.DEWEY["Y"] = "Young Adult"

    @classmethod
    def name_for(cls, identifier):
        return cls.NAMES.get(identifier, None)

    @classmethod
    def scrub_identifier(cls, identifier):
        if isinstance(identifier, int):
            identifier = str(identifier).zfill(3)

        identifier = identifier.lower()

        if ddc.startswith('[') and ddc.endswith(']'):
            # This is just bad data.
            ddc = ddc[1:-1]

        if ddc.startswith('c') or ddc.startswith('a'):
            # A work from our Canadian neighbors or our Australian
            # friends.
            ddc = ddc[1:]
        elif ddc.startswith("nz"):
            # A work from the good people of New Zealand.
            ddc = ddc[2:]

        # Trim everything after the first period. We don't know how to
        # deal with it.
        if '.' in identifier:
            identifier = identifier.split('.')[0]
        return identifier

    @classmethod
    def is_fiction(cls, identifier, name):
        """Is the given DDC classification likely to contain fiction?"""
        if identifier in ('e', 'fic'):
            # Juvenile fiction
            return True

        if identifier == 'j':
            # Juvenile non-fiction
            return False

        if identifier.startswith('f'):
            # Adult fiction
            return True

        if identifier == 'b':
            # Biography
            return False

        if identifier == 'y':
            # Inconsistently used for young adult fiction and
            # young adult nonfiction.
            return None

        if identifier.startswith('y') or identifier.startswith('j'):
            # Young adult/children's literature--not necessarily fiction
            identifier = identifier[1:]

        try:
            identifier = int(identifier)
        except Exception, e:
            return False
        if identifier in cls.FICTION:
            return True
        if identifier >= 830 and identifier <= 899:
            # TODO: make this less of a catch-all.
            return True

        return False

    @classmethod
    def audience(cls, identifier, name):
        if identifier in ('e', 'fic'):
            # Juvenile fiction
            return cls.AUDIENCE_CHILDREN

        if identifier.startswith('j'):
            return cls.AUDIENCE_CHILDREN

        if identifier.startswith('y'):
            return cls.AUDIENCE_YOUNG_ADULT

        return cls.AUDIENCE_ADULT

    @classmethod
    def lane(cls, identifier, name):

        if identifier in ('e', 'fic', 'j', 'b', 'y'):
            identifiers = [identifier]
        else:
            # Strip off everything except the three-digit number.
            identifier = identifier[-3:]
            try:
                # Turn a three-digit number into a top-level code.
                identifier = int(identifier)
                identifiers = [identifier, identifier / 100 * 100]
            except ValueError, e:
                # Oh well, try a lookup, maybe it'll work. (Probably not.)
                identifiers = [identifier]

        for identifier in identifiers:
            if identifier in cls.lane_for_identifier:
                return cls.lane_for_identifier[identifier]
        return None
    

class LCCClassification(Classification):

    TOP_LEVEL = re.compile("^([A-Z]{1,2})")
    FICTION = set(["P", "PN", "PQ", "PR", "PS", "PT", "PZ"])
    JUVENILE = set(["PZ"])

    LANES = {
        lane.Cooking : (
            [], set(["TX"])
        ),

        lane.FineArts : (
            [re.compile("[MN].*")],
            [],
        ),

        lane.History : (
            [re.compile("[CDEF].*")],
            set(["AZ", "KBR", "LA"]),
        ),

        lane.Philosophy : (
            [],
            set(["B", "BC", "BD"]),
        ),

        lane.Religion : (
            [],
            set([
                "KB", "KBM", "KBP", "KBR", "KBU",
                "BL", "BM", "BP", "BQ", "BR", "BS", "BT", "BV",
                "BX",
            ])
        ),

        lane.Science: (
            [re.compile("[QRS].*"), re.compile("T[A-P].*")],
            [],
        ),
    }

    NAMES = {}
    @classmethod
    def _load(cls):
        cls.NAMES = json.loads(
            pkgutil.get_data("resources", "lcc_one_level.json"))

    @classmethod
    def scrub_identifier(cls, identifier):
        # We don't currently have an understanding of anything
        # beyond the first two characters of an LCC identifier.
        return identifier[:2].upper()

    @classmethod
    def name_for(cls, identifier):
        return cls.NAMES.get(identifier, None)

    @classmethod
    def is_fiction(cls, identifier, name):
        return identifier in cls.FICTION

    @classmethod
    def lane(cls, identifier, name):
        for lane, (res, strings) in cls.LANES.items():
            if identifier in strings:
                return lane
            for r in res:
                if r.match(identifier):
                    return lane
        return None

    @classmethod
    def audience(cls, identifier, name):
        if identifier in cls.JUVENILE:
            return cls.AUDIENCE_CHILDREN
        # Everything else is implicitly for adults.
        return cls.AUDIENCE_ADULT

def match_kw(*l):
    """Turn a list of strings into a regular expression which matches
    any of those strings, so long as there's a word boundary on both ends.
    """
    any_keyword = "|".join([keyword for keyword in l])
    with_boundaries = r'\b(%s)\b' % any_keyword
    return re.compile(with_boundaries, re.I)

class KeywordBasedClassification(Classification):

    """Classify a book based on keywords."""
    
    FICTION_INDICATORS = match_kw("fiction", "stories", "tales", "literature")
    NONFICTION_INDICATORS = match_kw(
        "history", "biography", "histories", "biographies", "autobiography",
        "autobiographies")
    JUVENILE_INDICATORS = match_kw("for children", "children's", "juvenile")
    YOUNG_ADULT_INDICATORS = match_kw("young adult", "ya")

    LANES = {
        lane.Adventure : match_kw(
            "adventure",
            "western stories",
            "adventurers",
            "sea stories",
            "war stories",
        ),
        lane.Biography : match_kw(
            "autobiographies",
            "autobiography",
            "biographies",
            "biography",
        ),
        lane.Cooking : match_kw(
            "baking",
            "cookbook",
            "cooking",
            "food",
            "home economics",
        ),
        lane.Drama : match_kw(
            "drama",
            "plays",
        ),
        lane.Fantasy : match_kw(
            "fantasy",
        ),
        lane.History : match_kw(
            "histories",
            "history",
        ),
        lane.Horror : match_kw(
            "ghost stories",
            "horror",
        ),
        lane.Humor : match_kw(
            "comedies",
            "comedy",
            "humor",
            "humorous",
            "satire",
            "wit",
        ),
        lane.Mystery : match_kw(
            "crime",
            "detective",
            "murder",
            "mystery",
            "mysteries",
        ),
        lane.Periodicals : match_kw(
            "periodicals",
        ),
        lane.Philosophy : match_kw(
            "philosophy",
            "political science",
        ),
        lane.Poetry : match_kw(
            "poetry",
        ),
        lane.Reference : match_kw(
            "catalogs",
            "dictionaries",
            "encyclopedias",
            "handbooks",
            "manuals",
        ),
        lane.Religion : match_kw(
            "bible",
            "christianity",
            "church",
            "islam",
            "judaism",
            "religion",
            "religious",
            "sermons",
            "theological",
            "theology",
            'biblical',
        ),
        lane.Romance : match_kw(
            "love stories",
            "romance",
            "romances",
        ),
        lane.Science : match_kw(
            "aeronautics",
            "evolution",
            "mathematics",
            "medicine",
            "natural history",
            "science",
        ),
        lane.ScienceFiction : match_kw(
            "science fiction",
        ),
        lane.Travel : match_kw(
            "discovery",
            "exploration",
            "travel",
            "travels",
            "voyages",
        ),
        
    }

    @classmethod
    def is_fiction(cls, identifier, name):
        if not name:
            return None
        if (cls.FICTION_INDICATORS.search(name)):
            return True
        if (cls.NONFICTION_INDICATORS.search(name)):
            return False
        return None

    @classmethod
    def audience(cls, identifier, name):
        if name is None:
            return None
        if cls.JUVENILE_INDICATORS.search(name):
            return cls.AUDIENCE_CHILDREN
        if cls.YOUNG_ADULT_INDICATORS.search(name):
            return cls.AUDIENCE_YOUNG_ADULT
        return None

    @classmethod
    def lane(cls, identifier, name):
        match_against = [name]
        for lane, keywords in cls.LANES.items():
            print keywords.pattern, name, keywords.search(name)
            if keywords.search(name):
                print
                return lane
        return None

class LCSHClassification(KeywordBasedClassification):
    pass

class FASTClassification(KeywordBasedClassification):
    pass
