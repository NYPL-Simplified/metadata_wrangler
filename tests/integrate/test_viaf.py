import pkgutil
import StringIO
from integration.oclc import (
    OCLCXMLParser,
)
from nose.tools import set_trace, eq_

from ...core.model import (
    Contributor,
    )

from ...core.testing import (
    DatabaseTest,
)

from ...integration.viaf import (
    VIAFParser,
    VIAFClient,
)

class TestNameParser(DatabaseTest):

    def setup(self):
        super(TestNameParser, self).setup()
        self.parser = VIAFParser()

    def test_entry_with_wikipedia_name(self):

        xml = pkgutil.get_data(
            "tests.integrate",
            "files/viaf/will_eisner.xml")

        contributor, new = self._contributor(None)

        viaf, display, family, wikipedia = self.parser.info(contributor, xml, None)
        eq_("10455", viaf)
        eq_("Will Eisner", display)
        eq_("Eisner", family)
        eq_("Will_Eisner", wikipedia)

    def test_entry_without_wikipedia_name(self):
        xml = pkgutil.get_data(
            "tests.integrate",
            "files/viaf/palmer.xml")

        viaf, display, family, wikipedia = self.parser.parse(xml)
        eq_("2506349", viaf)
        eq_("Roy Ernest Palmer", display)
        eq_("Palmer", family)
        eq_(None, wikipedia)

    def test_simple_corporate_entry(self):
        xml = pkgutil.get_data(
            "tests.integrate",
            "files/viaf/aquarius.xml")
        viaf, display, family, wikipedia = self.parser.parse(xml)
        eq_("159591140", viaf)
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

        viaf, display, family, wikipedia = self.parser.parse(
            xml, "Sam Clemens")
        eq_("50566653", viaf)
        eq_("Mark Twain", display)
        eq_("Twain", family)
        eq_("Mark_Twain", wikipedia)

        # Let's try again without the Wikipedia name.
        xml = pkgutil.get_data(
            "tests.integrate",
            "files/viaf/mark_twain_no_wikipedia.xml")

        # The author is better known as Mark Twain, so this 
        # name wins by popularity if we don't specify a name going in.
        viaf, display, family, wikipedia = self.parser.parse(xml, None)
        eq_("50566653", viaf)
        eq_("Mark Twain", display)
        eq_("Twain", family)
        eq_(None, wikipedia)

        # But if we go in expecting something like "Sam Clemens",
        # that's what we'll get.
        viaf, display, family, wikipedia = self.parser.parse(
            xml, "Sam Clemens")
        eq_("50566653", viaf)
        eq_("Samuel Langhorne Clemens", display)
        eq_("Clemens", family)
        eq_(None, wikipedia)
        
    def test_ignore_results_if_author_not_in_viaf(self):
        # This is the VIAF result for searching for "Howard,
        # J. J.". There are lots of results but none of them is the
        # correct one. This test verifies that we ignore all the
        # incorrect results.
        xml = pkgutil.get_data(
            "tests.integrate",
            "files/viaf/howard_j_j.xml")
        name = "Howard, J. J."
        contributor, new = self._contributor(name)
        viaf, display_name, family_name, wikipedia_name = self.parser.info(
            contributor, xml, True)
        # We can't find a VIAF number. The display name and family name
        # are obtained through heuristics.
        eq_(None, viaf)
        eq_("J. J. Howard", display_name)
        eq_("Howard", family_name)
        eq_(None, wikipedia_name)

    def test_multiple_results_with_success(self):
        xml = pkgutil.get_data(
            "tests.integrate",
            "files/viaf/lancelyn_green.xml")
        name = "Green, Roger Lancelyn"
        contributor, new = self._contributor(name)
        viaf, display_name, family_name, wikipedia_name = self.parser.info(
            contributor, xml, True)
        eq_("29620265", viaf)
        eq_("Roger Lancelyn Green", display_name)
        eq_("Green", family_name)
        eq_("Roger_Lancelyn_Green", wikipedia_name)

    def test_multiple_results_with_viaf_number_but_no_name(self):
        # This author's VIAF entry doesn't have any name information
        # we don't already have, but it does have a VIAF entry, and we
        # pick that up.
        xml = pkgutil.get_data(
            "tests.integrate",
            "files/viaf/kate_lister.xml")
        name = "Lister, Kate"
        contributor, new = self._contributor(name)
        viaf, display_name, family_name, wikipedia_name = self.parser.info(
            contributor, xml, True)
        eq_("68169992", viaf)
        eq_("Kate Lister", display_name)
        eq_("Lister", family_name)
        eq_(None, wikipedia_name)
