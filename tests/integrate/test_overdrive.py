# encoding: utf-8
from nose.tools import (
    set_trace, eq_,
    assert_raises,
)
import pkgutil
import json
from tests.db import DatabaseTest
from integration.overdrive import (
    OverdriveAPI,
    OverdriveRepresentationExtractor,
    OverdriveBibliographicMonitor,
)
from model import (
    DataSource,
    Measurement,
    Resource,
    Identifier,
)

class TestOverdriveAPI(DatabaseTest):

    def test_availability_info(self):
        data = pkgutil.get_data(
            "tests.integrate",
            "files/overdrive/overdrive_book_list.json")
        raw = json.loads(data)
        availability = OverdriveRepresentationExtractor.availability_link_list(
            raw)
        for item in availability:
            for key in 'availability_link', 'id', 'title':
                assert key in item

    def test_update_new_licensepool(self):
        data = pkgutil.get_data(
            "tests.integrate",
            "files/overdrive/overdrive_availability_information.json")
        raw = json.loads(data)        

        # Create an identifier
        identifier = self._identifier(
            identifier_type=Identifier.OVERDRIVE_ID
        )

        # Make it look like the availability information is for the
        # newly created Identifier.
        raw['id'] = identifier.identifier

        api = OverdriveAPI(self._db)
        pool, was_new, changed = api.update_licensepool_with_book_info(raw)
        eq_(True, was_new)
        eq_(True, changed)

        # The title of the corresponding Edition has been filled
        # in, just to provide some basic human-readable metadata.
        eq_("Blah blah blah", pool.edition().title)
        eq_(raw['copiesOwned'], pool.licenses_owned)
        eq_(raw['copiesAvailable'], pool.licenses_available)
        eq_(0, pool.licenses_reserved)
        eq_(raw['numberOfHolds'], pool.patrons_in_hold_queue)

    def test_update_existing_licensepool(self):
        data = pkgutil.get_data(
            "tests.integrate",
            "files/overdrive/overdrive_availability_information.json")
        raw = json.loads(data)        

        # Create a LicensePool.
        wr, pool = self._edition(
            data_source_name=DataSource.OVERDRIVE,
            identifier_type=Identifier.OVERDRIVE_ID,
            with_license_pool=True
        )

        # Make it look like the availability information is for the
        # newly created LicensePool.
        raw['id'] = pool.identifier.identifier

        wr.title = "The real title."
        eq_(0, pool.licenses_owned)
        eq_(0, pool.licenses_available)
        eq_(0, pool.licenses_reserved)
        eq_(0, pool.patrons_in_hold_queue)

        api = OverdriveAPI(self._db)
        p2, was_new, changed = api.update_licensepool_with_book_info(raw)
        eq_(False, was_new)
        eq_(True, changed)
        eq_(p2, pool)
        # The title didn't change to that title given in the availability
        # information, because we already set a title for that work.
        eq_("The real title.", wr.title)
        eq_(raw['copiesOwned'], pool.licenses_owned)
        eq_(raw['copiesAvailable'], pool.licenses_available)
        eq_(0, pool.licenses_reserved)
        eq_(raw['numberOfHolds'], pool.patrons_in_hold_queue)

    def test_update_licensepool_with_holds(self):
        data = pkgutil.get_data(
            "tests.integrate",
            "files/overdrive/overdrive_availability_information_holds.json")
        raw = json.loads(data)
        identifier = self._identifier(
            identifier_type=Identifier.OVERDRIVE_ID
        )
        raw['id'] = identifier.identifier

        api = OverdriveAPI(self._db)
        pool, was_new, changed = api.update_licensepool_with_book_info(raw)
        eq_(10, pool.patrons_in_hold_queue)
        eq_(True, changed)

    def test_link(self):
        data = pkgutil.get_data(
            "tests.integrate",
            "files/overdrive/overdrive_book_list.json")
        raw = json.loads(data)
        expect = OverdriveAPI.make_link_safe("http://api.overdrive.com/v1/collections/collection-id/products?limit=300&offset=0&lastupdatetime=2014-04-28%2009:25:09&sort=popularity:desc&formats=ebook-epub-open,ebook-epub-adobe,ebook-pdf-adobe,ebook-pdf-open")
        eq_(expect, OverdriveRepresentationExtractor.link(raw, "first"))

    def test_annotate_edition_with_bibliographic_information(self):

        wr, new = self._edition(with_license_pool=True)
        data = pkgutil.get_data(
            "tests.integrate",
            "files/overdrive/overdrive_metadata.json")
        info = json.loads(data)

        input_source = DataSource.lookup(self._db, DataSource.OVERDRIVE)
        OverdriveBibliographicMonitor.annotate_edition_with_bibliographic_information(
            self._db, wr, info, input_source)

        # Basic bibliographic info.
        eq_("Agile Documentation", wr.title)
        eq_("A Pattern Guide to Producing Lightweight Documents for Software Projects", wr.subtitle)
        eq_("Wiley Software Patterns", wr.series)
        eq_("eng", wr.language)
        eq_("Wiley", wr.publisher)
        eq_("John Wiley & Sons, Inc.", wr.imprint)
        eq_(2005, wr.published.year)
        eq_(1, wr.published.month)
        eq_(31, wr.published.day)

        # Author stuff
        author = wr.author_contributors[0]
        eq_(u"Rüping, Andreas", author.name)
        eq_("Andreas R&#252;ping", author.display_name)
        eq_(set(["Computer Technology", "Nonfiction"]),
            set([c.subject.identifier
                 for c in wr.primary_identifier.classifications]))

        # Related IDs.
        equivalents = [x.output for x in wr.primary_identifier.equivalencies]
        ids = [(x.type, x.identifier) for x in equivalents]
        eq_([("ASIN", "B000VI88N2"), ("ISBN", "9780470856246")],
            sorted(ids))

        # Associated resources.
        resources = wr.primary_identifier.resources
        eq_(3, len(resources))
        long_description = [
            x for x in resources if x.rel==Resource.DESCRIPTION
            and x.href=="tag:full"
        ][0]
        assert long_description.content.startswith("<p>Software documentation")

        short_description = [
            x for x in resources if x.rel==Resource.DESCRIPTION
            and x.href=="tag:short"
        ][0]
        assert short_description.content.startswith("<p>Software documentation")
        assert len(short_description.content) < len(long_description.content)

        image = [x for x in resources if x.rel==Resource.IMAGE][0]
        eq_('http://images.contentreserve.com/ImageType-100/0128-1/%7B3896665D-9D81-4CAC-BD43-FFC5066DE1F5%7DImg100.jpg', image.href)

        measurements = wr.primary_identifier.measurements
        popularity = [x for x in measurements
                      if x.quantity_measured==Measurement.POPULARITY][0]
        eq_(2, popularity.value)

        rating = [x for x in measurements
                  if x.quantity_measured==Measurement.RATING][0]
        eq_(1, rating.value)

        # Un-schematized metadata.

        eq_("eBook", wr.extra['medium'])
        eq_("Agile Documentation A Pattern Guide to Producing Lightweight Documents for Software Projects", wr.sort_title)


    def test_annotate_edition_with_sample(self):
        wr, new = self._edition(with_license_pool=True)
        data = pkgutil.get_data(
            "tests.integrate",
            "files/overdrive/overdrive_has_sample.json")
        info = json.loads(data)

        input_source = DataSource.lookup(self._db, DataSource.OVERDRIVE)
        OverdriveBibliographicMonitor.annotate_edition_with_bibliographic_information(
            self._db, wr, info, input_source)
        
        i = wr.primary_identifier
        [sample] = [x for x in i.resources if x.rel == Resource.SAMPLE]
        eq_("application/epub+zip", sample.media_type)
        eq_("http://excerpts.contentreserve.com/FormatType-410/1071-1/9BD/24F/82/BridesofConvenienceBundle9781426803697.epub", sample.href)
        eq_(820171, sample.file_size)

    def test_annotate_edition_with_awards(self):
        wr, new = self._edition(with_license_pool=True)
        data = pkgutil.get_data(
            "tests.integrate",
            "files/overdrive/overdrive_has_awards.json")
        info = json.loads(data)

        input_source = DataSource.lookup(self._db, DataSource.OVERDRIVE)
        OverdriveBibliographicMonitor.annotate_edition_with_bibliographic_information(
            self._db, wr, info, input_source)
        eq_(wr.extra['awards'], [{"source":"The New York Times","value":"The New York Times Best Seller List"}])

    def test_get_download_link(self):
        data = json.loads(pkgutil.get_data(
            "tests.integrate",
            "files/overdrive/checkout_response_locked_in_format.json"))
        url = OverdriveAPI.get_download_link(
            data, "ebook-epub-adobe", "http://foo.com/")
        eq_("http://patron.api.overdrive.com/v1/patrons/me/checkouts/76C1B7D0-17F4-4C05-8397-C66C17411584/formats/ebook-epub-adobe/downloadlink?errorpageurl=http://foo.com/", url)
        
        assert_raises(IOError, OverdriveAPI.get_download_link,
            data, "no-such-format", "http://foo.com/")

    def test_extract_data_from_checkout_resource(self):
        data = json.loads(pkgutil.get_data(
            "tests.integrate",
            "files/overdrive/checkout_response_locked_in_format.json"))
        expires, url = OverdriveAPI.extract_data_from_checkout_response(
            data, "ebook-epub-adobe", "http://foo.com/")
        eq_(2013, expires.year)
        eq_(10, expires.month)
        eq_(4, expires.day)
        eq_("http://patron.api.overdrive.com/v1/patrons/me/checkouts/76C1B7D0-17F4-4C05-8397-C66C17411584/formats/ebook-epub-adobe/downloadlink?errorpageurl=http://foo.com/", url)

    def test_sync_bookshelf_creates_local_loans(self):
        data = json.loads(pkgutil.get_data(
            "tests.integrate",
            "files/overdrive/shelf_with_some_checked_out_books.json"))
        
        # All four loans in the sample data were created.
        patron = self.default_patron
        loans = OverdriveAPI.sync_bookshelf(patron, data)
        eq_(4, len(loans))
        eq_(loans, patron.loans)

        # Running the sync again leaves all four loans in place.
        loans = OverdriveAPI.sync_bookshelf(patron, data)
        eq_(4, len(loans))
        eq_(loans, patron.loans)        

    def test_sync_bookshelf_removes_loans_not_present_on_remote(self):
        data = json.loads(pkgutil.get_data(
            "tests.integrate",
            "files/overdrive/shelf_with_some_checked_out_books.json"))
        
        patron = self.default_patron
        overdrive, new = self._edition(data_source_name=DataSource.OVERDRIVE,
                                       with_license_pool=True)
        overdrive_loan, new = overdrive.license_pool.loan_to(patron)

        # The loan not present in the sample data has been removed
        loans = OverdriveAPI.sync_bookshelf(patron, data)
        eq_(4, len(loans))
        eq_(loans, patron.loans)
        assert overdrive_loan not in patron.loans

    def test_sync_bookshelf_ignores_loans_from_other_sources(self):
        patron = self.default_patron
        gutenberg, new = self._edition(data_source_name=DataSource.GUTENBERG,
                                       with_license_pool=True)
        gutenberg_loan, new = gutenberg.license_pool.loan_to(patron)
        data = json.loads(pkgutil.get_data(
            "tests.integrate",
            "files/overdrive/shelf_with_some_checked_out_books.json"))
        
        # Overdrive doesn't know about the Gutenberg loan, but it was
        # not destroyed, because it came from another source.
        loans = OverdriveAPI.sync_bookshelf(patron, data)
        eq_(5, len(patron.loans))
        assert gutenberg_loan in patron.loans

