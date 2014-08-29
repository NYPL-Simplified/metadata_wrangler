import json
import pkgutil
from collections import (
    Counter,
    defaultdict,
)
from nose.tools import set_trace
import re

from util import MetadataSimilarity

from model import (
    Subject
)

from lane import LaneData

class AssignSubjectsToLanes(object):

    def __init__(self, _db):
        self._db = _db

    def run(self, force=False):
        q = self._db.query(Subject).filter(Subject.locked==False)
        if not force:
            q = q.filter(Subject.lane==None)
        for subject in q:
            classifier = Subject.classifiers.get(
                subject.type, GenericClassification)
            lane, audience, fiction = classifier.classify(subject)
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

    @classmethod
    def old_classify(self, subjects, normalize=False, counters={}):
        audience = counters.get('audience', Counter())
        fiction = counters.get('fiction', Counter())
        names = counters.get('names', defaultdict(Counter))
        codes = counters.get('codes', defaultdict(Counter))
        for type, classifier, key in (
                ('DDC', DeweyDecimalClassification, 'id'),
                ('LCC', LCCClassification, 'id'),
                ('Overdrive', OverdriveClassification, 'id'),
                ('FAST', FASTClassification, 'value'),
                ('LCSH', LCSHClassification, 'id'),):
            raw_subjects = subjects.get(type, [])
            for s in raw_subjects:
                value = s[key]
                weight = s.get('weight', 1)
                for (code, name, o_audience, o_fiction) in classifier.names(value):
                    fiction[o_fiction] += weight
                    audience[o_audience] += weight
                    names[type][name] += weight
                    codes[type][code] += weight
        n = MetadataSimilarity.normalize_histogram
        if normalize:
            audience = n(audience)
            fiction = n(fiction)
            for k, v in codes.items():
                codes[k] = n(v)
            for k, v in names.items():
                names[k] = n(v)
        return dict(audience=audience, fiction=fiction, codes=codes,
                    names=names)

class GenericClassification(Classification):

    @classmethod
    def scrub_identifier(cls, identifier):
        return identifier.lower()

    @classmethod
    def scrub_name(cls, name):
        return name.lower()

    @classmethod
    def classify(cls, subject):
        identifier = cls.scrub_identifier(subject.identifier)
        if subject.name:
            name = cls.scrub_name(subject.name)
        else:
            name = identifier
        return (cls.lane(identifier, name),
                cls.audience(identifier, name),
                cls.fiction(identifier, name))

    @classmethod
    def lane(cls, identifier, name):
        return None

    @classmethod
    def is_fiction(cls, identifier, name):
        if "Fiction" in name:
            return True
        if "Nonfiction" in name:
            return False
        return None

    @classmethod
    def audience(cls, identifier, name):
        if 'Juvenile' in name:
            return cls.AUDIENCE_CHILDREN
        elif 'Young Adult' in name:
            return cls.AUDIENCE_YOUNG_ADULT
        else:
            return cls.AUDIENCE_ADULT

class OverdriveClassification(GenericClassification):
    pass

class DeweyDecimalClassification(Classification):

    DEWEY = None

    FICTION_CLASSIFICATIONS = (
        800, 810, 811, 812, 813, 817, 820, 821, 822, 823, 827)

    lane_for_identifier = dict()
    for lane in LaneData.SELF_AND_SUBLANES:
        for identifier in lane.DDC:
            if isinstance(identifier, range):
                for i in identifier:
                    lane_for_identifier[i] = lane
            else:
                lane_for_identifier[identifier] = lane

    @classmethod
    def _load(cls):
        cls.DEWEY = json.loads(
            pkgutil.get_data("resources", "dewey_1000.json"))

        # Add some other values commonly found in MARC records.
        cls.DEWEY["B"] = "Biography"
        cls.DEWEY["E"] = "Juvenile Fiction"
        cls.DEWEY["FIC"] = "Juvenile Fiction"

    @classmethod
    def scrub_identifier(cls, identifier):
        identifier = identifier.lower()

        if ddc.startswith('[') and ddc.endswith(']'):
            # This is just bad data.
            ddc = ddc[1:-1]

        if ddc.startswith('c') or ddc.startswith('a'):
            # A work from our Canadian neighbors or our Australian
            # friends. It's all the same to us!
            ddc = ddc[1:]
        elif ddc.startswith("nz"):
            # A work from the good people of New Zealand.
            ddc = ddc[2:]

        # Trim everything after the first period.
        if '.' in identifier:
            identifier = identifier.split('.')[0]
        return identifier

    @classmethod
    def is_fiction(cls, identifier, name):
        """Is the given DDC classification likely to contain fiction?"""
        if isinstance(identifier, int):
            identifier = str(identifier).zfill(3)

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
        if identifier in cls.FICTION_CLASSIFICATIONS:
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
            if identifier in self.lane_for_identifier:
                return self.lane_for_identifier[identifier]
        return None
    

class LCCClassification(Classification):

    TOP_LEVEL = re.compile("^([A-Z]{1,2})")
    FICTION = set(["P", "PN", "PQ", "PR", "PS", "PT", "PZ"])
    LCC = None

    @classmethod
    def _load(cls):
        cls.LCC = json.loads(
            pkgutil.get_data("resources", "lcc_one_level.json"))

    @classmethod
    def lookup(cls, identifier):
        """Do a direct identifier-value lookup."""
        if not cls.LCC:
            cls._load()
        return cls.LCC.get(identifier.upper(), None)

    @classmethod
    def is_fiction(cls, identifier):
        return identifier.upper() in cls.FICTION

    @classmethod
    def names(cls, lcc, name):
        """Yield increasingly more specific classifications for the given number.

        Yields 4-tuples:
         (code, human_readable_name, audience, fiction).
        """
        # Just pick the first two alphabetic characters. We don't have
        # the information necessary to do a lookup in any more detail.

        tl = cls.TOP_LEVEL.search(lcc)
        if not tl:
            return
        lcc = tl.groups()[0]
        if lcc == "PZ":
            audience = cls.AUDIENCE_CHILDREN
        else:
            audience = cls.AUDIENCE_ADULT
        if len(lcc) > 1:
            code = lcc[0]
            value = cls.lookup(code)
            if value:
                yield (code, value, audience, 
                       cls.is_fiction(code))
        value = cls.lookup(lcc)
        if value:
            yield (lcc, value, audience, 
                   cls.is_fiction(lcc))


class LCSHClassification(Classification):

    FICTION_INDICATORS = set(["fiction", "stories", "tales", "literature"])
    NONFICTION_INDICATORS = set(["history", "biography"])
    JUVENILE_INDICATORS = set(["for children", "children's", "juvenile"])

    @classmethod
    def split(cls, lcshes):
        for i in lcshes.split("--"):
            yield i.strip()

    @classmethod
    def is_fiction(cls, name):
        if name is None:
            return None
        name = name.lower()
        for i in cls.NONFICTION_INDICATORS:
            if i in name:
                return False
        for i in cls.FICTION_INDICATORS:
            if i in name:
                return True
        return None

    @classmethod
    def audience(cls, name):
        if name is None:
            return None
        name = name.lower()
        for i in cls.JUVENILE_INDICATORS:
            if i in name:
                return cls.AUDIENCE_CHILDREN
        return None
        
    @classmethod
    def names(cls, lcsh, name):
        for name in cls.split(lcsh):
            yield (name, name, cls.audience(name), cls.is_fiction(name))

class FASTClassification(LCSHClassification):
    """By and large, LCSH rules also apply to FAST."""

    @classmethod
    def names(cls, fast_id, fast_name):
        # Since FAST classifications have IDs associated with them,
        # don't try to split them into parts the way we do with LCSH.
        # We don't know what the IDs are! TODO: But mabye we could
        # have that information available?
        yield (fast_id, fast_name, cls.audience(fast_name), cls.is_fiction(fast_name))
