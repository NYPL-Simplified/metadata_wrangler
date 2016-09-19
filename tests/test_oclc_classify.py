from nose.tools import eq_, set_trace

from . import DatabaseTest

from oclc_classify import OCLCClassifyCoverageProvider

class TestOCLCClassifyCoverageProvider(DatabaseTest):

    def setup(self):
        super(TestOCLCClassifyCoverageProvider, self).setup()

        self.provider = OCLCClassifyCoverageProvider(self._db)

    def test_oclc_safe_title(self):
        # Returns an empty string when passed None.
        eq_(self.provider.oclc_safe_title(None), '')

        # Returns the original title if it has no special characters.
        title = 'The Curious Incident of the Dog in the Night-Time'
        eq_(self.provider.oclc_safe_title(title), title)

        # Returns a title without special characters otherwise.
        title = '3 Blind Mice & Other Tales: A Bedtime Reader'
        expected = '3 Blind Mice  Other Tales A Bedtime Reader'
        eq_(self.provider.oclc_safe_title(title), expected)

