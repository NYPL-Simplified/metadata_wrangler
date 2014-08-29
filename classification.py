import json
import pkgutil
from collections import (
    Counter,
    defaultdict,
)
from nose.tools import set_trace
import re

from util import MetadataSimilarity

class Classification(object):

    AUDIENCE_CHILDREN = "Children"
    AUDIENCE_YA = "Young Adult"
    AUDIENCE_ADULT = "Adult"

    @classmethod
    def classify(self, subjects, normalize=False, counters={}):
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
    def is_fiction(cls, key):
        if "Fiction" in key:
            return True
        if "Nonfiction" in key:
            return False
        return None

    @classmethod
    def audience(cls, key):
        if 'Juvenile' in key:
            return cls.AUDIENCE_CHILDREN
        elif 'Young Adult' in key:
            return cls.AUDIENCE_YOUNG_ADULT
        else:
            return cls.AUDIENCE_ADULT

    @classmethod
    def names(cls, key, name):
        yield (key, name, cls.audience(key), 
               cls.is_fiction(key))

class OverdriveClassification(GenericClassification):
    pass

class DeweyDecimalClassification(Classification):

    DEWEY = None

    @classmethod
    def _load(cls):
        cls.DEWEY = json.loads(
            pkgutil.get_data("resources", "dewey_1000.json"))

        # Add some other values commonly found in MARC records.
        cls.DEWEY["B"] = "Biography"
        cls.DEWEY["E"] = "Juvenile Fiction"
        cls.DEWEY["FIC"] = "Juvenile Fiction"

    @classmethod
    def is_fiction(cls, key):
        """Is the given DDC classification likely to contain fiction?"""
        if isinstance(key, int):
            key = str(key).zfill(3)

        key = key.upper()
        if key in ('E', 'FIC'):
            # Juvenile fiction
            return True
        elif key == 'B':
            # Biography
            return False

        if key.startswith('J'):
            key = key[1:]

        if key.startswith('F'):
            # Adult fiction
            return True

        if '.' in key:
            key = key.split('.')[0]

        try:
            key = int(key)
        except Exception, e:
            return False
        if key in (800, 810, 811, 812, 813, 817, 820, 821, 822, 823, 827):
            return True
        if key >= 830 and key <= 899:
            # TODO: make this less of a catch-all.
            return True

        return False

    @classmethod
    def lookup(cls, key):
        """Do a direct key-value lookup."""
        if not cls.DEWEY:
            cls._load()
        return cls.DEWEY.get(key.upper(), None)

    @classmethod
    def names(cls, ddc, name):
        """Yield increasingly more specific classifications for the given number.

        Yields 4-tuples:
         (code, human_readable_name, audience, fiction).
        """

        ddc = ddc.upper()

        if ddc.startswith('[') and ddc.endswith(']'):
            ddc = ddc[1:-1]

        if ddc.startswith('C') or ddc.startswith('A'):
            # Indicates a Canadian or Australian work. It doesnt'
            # matter to us.
            ddc = ddc[1:]
        elif ddc.startswith("NZ"):
            # New Zealand.
            ddc = ddc[2:]

        audience = cls.AUDIENCE_ADULT
        if ddc == 'J':
            yield (ddc, cls.lookup(ddc), cls.AUDIENCE_CHILDREN, False)
            return
        if ddc.startswith('J'):
            audience = cls.AUDIENCE_CHILDREN
            new_ddc = ddc[1:]
            yield (ddc, cls.lookup(new_ddc), cls.AUDIENCE_CHILDREN,
                   cls.is_fiction(new_ddc))
            ddc = new_ddc

        if ddc.startswith('Y'):
            audience = cls.AUDIENCE_YOUNG_ADULT
            new_ddc = ddc[1:]
            yield (ddc, cls.lookup(new_ddc), cls.AUDIENCE_YOUNG_ADULT,
                   cls.is_fiction(new_ddc))
            ddc = new_ddc

        if ddc in ('E', 'FIC'):
            audience = cls.AUDIENCE_CHILDREN
            yield (ddc, cls.lookup(ddc), audience, cls.is_fiction(ddc))
            return

        if ddc == 'B':
            yield (ddc, cls.lookup(ddc), audience, cls.is_fiction(ddc))
            return
        elif ddc.startswith("F"):
            audience = cls.AUDIENCE_ADULT
            is_fiction = True
            ddc = ddc[1:]

        if not ddc:
            yield (None, None, audience, is_fiction)
            return

        # At this point we should only have dotted-number
        # classifications left.

        parts = ddc.split(".")

        # First get the top-level classification
        try:
            first_part = int(parts[0])
        except Exception, e:
            yield (None, None, audience, is_fiction)
            return
        top_level = str(first_part / 100 * 100)
        yield (top_level, cls.lookup(top_level), audience,
               cls.is_fiction(top_level))

        # Now go one set of dots at a time.
        working_ddc = ''
        for i, part in enumerate(parts):
            if not working_ddc and i == 0 and part == top_level:
                continue
            if working_ddc:
                working_ddc += '.'
            working_ddc += parts[i]
            value = cls.lookup(working_ddc)
            if value:
                yield (working_ddc, value, audience, 
                       cls.is_fiction(working_ddc))

class LCCClassification(Classification):

    TOP_LEVEL = re.compile("^([A-Z]{1,2})")
    FICTION = set(["P", "PN", "PQ", "PR", "PS", "PT", "PZ"])
    LCC = None

    @classmethod
    def _load(cls):
        cls.LCC = json.loads(
            pkgutil.get_data("resources", "lcc_one_level.json"))

    @classmethod
    def lookup(cls, key):
        """Do a direct key-value lookup."""
        if not cls.LCC:
            cls._load()
        return cls.LCC.get(key.upper(), None)

    @classmethod
    def is_fiction(cls, key):
        return key.upper() in cls.FICTION

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
