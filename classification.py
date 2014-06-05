import json
import pkgutil
from nose.tools import set_trace
import re

class Classification(object):

    AUDIENCE_CHILDREN = "Children"
    AUDIENCE_YA = "Young Adult"
    AUDIENCE_ADULT = "Adult"

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

        if '.' in key:
            key = key.split('.')[0]

        key = int(key)
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
        set_trace()

    @classmethod
    def names(cls, ddc):
        """Yield increasingly more specific classifications for the given number.

        Yields 4-tuples:
         (code, human_readable_name, audience, fiction).
        """

        ddc = ddc.upper()

        audience = cls.AUDIENCE_ADULT
        if ddc.startswith('J'):
            audience = cls.AUDIENCE_CHILDREN
            ddc = ddc[1:]

        if ddc in ('E', 'FIC'):
            audience = cls.AUDIENCE_CHILDREN
            yield (ddc, cls.lookup(ddc), audience, cls.is_fiction(ddc))
            return

        elif ddc == 'B':
            yield (ddc, cls.lookup(ddc), audience, cls.is_fiction(ddc))
            return

        # At this point we should only have dotted-number
        # classifications left.

        parts = ddc.split(".")

        # First get the top-level classification
        first_part = int(parts[0])
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
    def names(cls, lcc):
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

