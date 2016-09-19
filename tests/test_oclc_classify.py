from nose.tools import eq_, set_trace

from . import (
    DatabaseTest,
    sample_data,
)

from core.coverage import CoverageFailure
from core.model import Contributor

from oclc_classify import (
    OCLCClassifyCoverageProvider
)
from testing import MockOCLCClassifyAPI


class TestOCLCClassifyCoverageProvider(DatabaseTest):

    def setup(self):
        super(TestOCLCClassifyCoverageProvider, self).setup()

        self.edition = self._edition(with_license_pool=True)[0]
        self.identifier = self.edition.primary_identifier
        self.api = MockOCLCClassifyAPI()
        self.provider = OCLCClassifyCoverageProvider(self._db, api=self.api)

    def sample_data(self, filename):
        return sample_data(filename, 'oclc_classify')

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

    def test_process_item_without_book_information(self):
        def process_item():
            lookup = self.sample_data('jane_eyre.xml')
            self.api.queue_lookup(lookup)
            return self.provider.process_item(self.identifier)

        # Create an edition without a title
        self.edition.title = None

        result = process_item()
        eq_(True, isinstance(result, CoverageFailure))
        eq_(True, result.exception.endswith('title and author!'))

        # Create an edition without an author
        self.edition.title = "Jane Eyre"
        self._db.delete(self.edition.contributions[0])
        self._db.commit()

        result = process_item()
        eq_(True, isinstance(result, CoverageFailure))
        eq_(True, result.exception.endswith('title and author!'))

        # Create an edition with both a title and author
        bronte = self._contributor(sort_name="Bronte, Charlotte")[0]
        self.edition.add_contributor(bronte, Contributor.AUTHOR_ROLE)

        result = process_item()
        eq_(result, self.identifier)

    def test_process_item_when_parsing_error_occurs(self):
        class AlwaysErrorsClassifyProvider(OCLCClassifyCoverageProvider):
            def parse_edition_data(self, *args, **kwargs):
                raise IOError('It broke!')

        provider = AlwaysErrorsClassifyProvider(self._db, api=self.api)
        self.api.queue_lookup(self.sample_data('jane_eyre.xml'))
        result = provider.process_item(self.identifier)

        eq_(True, isinstance(result, CoverageFailure))
        eq_(self.identifier, result.obj)
        eq_('It broke!', result.exception)
        eq_(provider.output_source, result.data_source)
