import pkgutil
import StringIO
from integration.oclc import (
    OCLCXMLParser,
)
from nose.tools import set_trace, eq_

from model import (
    Contributor,
    )

from tests.db import (
    DatabaseTest,
)

from integration.viaf import (
    VIAFParser,
    VIAFClient,
)

# 288540365 | 288540365 | uncontrolled

class TestNameParser(object):

    def setup(self):
        self.parser = VIAFParser()

    def test_entry_with_wikipedia_name(self):

        xml = pkgutil.get_data(
            "tests.integrate",
            "files/viaf/will_eisner.xml")

        display, family, wikipedia = self.parser.parse(xml)
        eq_("Will Eisner", display)
        eq_("Eisner", family)
        eq_("Will_Eisner", wikipedia)

    def test_entry_without_wikipedia_name(self):
        xml = pkgutil.get_data(
            "tests.integrate",
            "files/viaf/palmer.xml")

        display, family, wikipedia = self.parser.parse(xml)
        eq_("Roy Ernest Palmer", display)
        eq_("Palmer", family)
        eq_(None, wikipedia)

    def test_simple_corporate_entry(self):
        xml = pkgutil.get_data(
            "tests.integrate",
            "files/viaf/aquarius.xml")
        display, family, wikipedia = self.parser.parse(xml)
        eq_("Aquarius Paris", display)
        eq_("Aquarius", family)
        eq_(None, wikipedia)

    def test_many_names(self):
        # Even if we pass in "Sam Clemens" as the working name,
        # the family name we get back is "Twain", because the Wikipedia
        # name takes precedence over the working name.
        xml = pkgutil.get_data(
            "tests.integrate",
            "files/viaf/mark_twain.xml")

        display, family, wikipedia = self.parser.parse(xml, "Sam Clemens")
        eq_("Mark Twain", display)
        eq_("Twain", family)
        eq_("Mark_Twain", wikipedia)

        # Let's try again without the Wikipedia name.
        xml = pkgutil.get_data(
            "tests.integrate",
            "files/viaf/mark_twain_no_wikipedia.xml")

        # The author is better known as Mark Twain, so this 
        # name wins by popularity.
        display, family, wikipedia = self.parser.parse(xml)
        eq_("Mark Twain", display)
        eq_("Twain", family)
        eq_(None, wikipedia)

        # But if we go in expecting something like "Sam Clemens",
        # that's what we'll get.
        display, family, wikipedia = self.parser.parse(xml, "Sam Clemens")
        eq_("Samuel Langhorne Clemens", display)
        eq_("Clemens", family)
        eq_(None, wikipedia)
        
