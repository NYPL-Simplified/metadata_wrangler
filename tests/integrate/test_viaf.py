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

    def test_corporate_entry(self):
        xml = pkgutil.get_data(
            "tests.integrate",
            "files/viaf/us_congress.xml")
        display, family, wikipedia = self.parser.parse(xml)
        eq_("House of representatives Etats-Unis", display)
        eq_("Etats-Unis", family)
        eq_(None, wikipedia)

        # That's no good. We don't want no French names. But that's
        # what VIAF gives us in this case. Fortunately, if we give the
        # parser a provisional name, it will use that name to find a
        # corresponding VIAF name.
        display, family, wikipedia = self.parser.parse(
            xml, "House of Representatives")
        eq_("House of Representatives Estados Unidos.", display)
        eq_("Estados Unidos.", family)
        eq_(None, wikipedia)

        # That's not good either. Let's try a different provisional name
        display, family, wikipedia = self.parser.parse(xml, "Congress")
        eq_(u"Congress E\u0301tats-Unis", display)
        eq_(u"E\u0301tats-Unis", family)
        eq_(None, wikipedia)

        # Unfortunatley at the moment there's no way to get a good,
        # reliable American name out of the parser. But I hope you had
        # a good time learning about how it works.
