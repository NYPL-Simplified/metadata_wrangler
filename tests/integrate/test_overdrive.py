# encoding: utf-8
from nose.tools import set_trace, eq_
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
    WorkIdentifier,
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
        identifier = self._workidentifier(
            identifier_type=WorkIdentifier.OVERDRIVE_ID
        )

        # Make it look like the availability information is for the
        # newly created WorkIdentifier.
        raw['id'] = identifier.identifier

        overdrive = DataSource.lookup(self._db, DataSource.OVERDRIVE)

        pool, was_new = OverdriveAPI.update_licensepool_with_book_info(
            self._db, overdrive, raw
            )
        eq_(True, was_new)

        # The title of the corresponding WorkRecord has been filled
        # in, just to provide some basic human-readable metadata.
        eq_("Blah blah blah", pool.work_record().title)
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
        wr, pool = self._workrecord(
            data_source_name=DataSource.OVERDRIVE,
            identifier_type=WorkIdentifier.OVERDRIVE_ID,
            with_license_pool=True
        )

        # Make it look like the availability information is for the
        # newly created LicensePool.
        raw['id'] = pool.identifier.identifier

        overdrive = DataSource.lookup(self._db, DataSource.OVERDRIVE)

        wr.title = "The real title."
        eq_(0, pool.licenses_owned)
        eq_(0, pool.licenses_available)
        eq_(0, pool.licenses_reserved)
        eq_(0, pool.patrons_in_hold_queue)

        p2, was_new = OverdriveAPI.update_licensepool_with_book_info(
            self._db, overdrive, raw
            )
        eq_(False, was_new)
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
        identifier = self._workidentifier(
            identifier_type=WorkIdentifier.OVERDRIVE_ID
        )
        raw['id'] = identifier.identifier

        overdrive = DataSource.lookup(self._db, DataSource.OVERDRIVE)
        pool, was_new = OverdriveAPI.update_licensepool_with_book_info(
            self._db, overdrive, raw
            )
        eq_(10, pool.patrons_in_hold_queue)

    def test_link(self):
        data = pkgutil.get_data(
            "tests.integrate",
            "files/overdrive/overdrive_book_list.json")
        raw = json.loads(data)
        expect = OverdriveAPI.make_link_safe("http://api.overdrive.com/v1/collections/collection-id/products?limit=300&offset=0&lastupdatetime=2014-04-28%2009:25:09&sort=popularity:desc&formats=ebook-epub-open,ebook-epub-adobe,ebook-pdf-adobe,ebook-pdf-open")
        eq_(expect, OverdriveRepresentationExtractor.link(raw, "first"))

    def test_annotate_work_record_with_bibliographic_information(self):

        wr, new = self._workrecord(with_license_pool=True)
        data = pkgutil.get_data(
            "tests.integrate",
            "files/overdrive/overdrive_metadata.json")
        info = json.loads(data)

        input_source = DataSource.lookup(self._db, DataSource.OVERDRIVE)
        OverdriveBibliographicMonitor.annotate_work_record_with_bibliographic_information(
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


    def test_annotate_work_record_with_sample(self):
        wr, new = self._workrecord(with_license_pool=True)
        data = pkgutil.get_data(
            "tests.integrate",
            "files/overdrive/overdrive_has_sample.json")
        info = json.loads(data)

        input_source = DataSource.lookup(self._db, DataSource.OVERDRIVE)
        OverdriveBibliographicMonitor.annotate_work_record_with_bibliographic_information(
            self._db, wr, info, input_source)
        
        i = wr.primary_identifier
        [sample] = [x for x in i.resources if x.rel == Resource.SAMPLE]
        eq_("application/epub+zip", sample.media_type)
        eq_("http://excerpts.contentreserve.com/FormatType-410/1071-1/9BD/24F/82/BridesofConvenienceBundle9781426803697.epub", sample.href)
        eq_(820171, sample.file_size)

    def test_annotate_work_record_with_awards(self):
        wr, new = self._workrecord(with_license_pool=True)
        data = pkgutil.get_data(
            "tests.integrate",
            "files/overdrive/overdrive_has_awards.json")
        info = json.loads(data)

        input_source = DataSource.lookup(self._db, DataSource.OVERDRIVE)
        OverdriveBibliographicMonitor.annotate_work_record_with_bibliographic_information(
            self._db, wr, info, input_source)
        eq_(wr.extra['awards'], [{"source":"The New York Times","value":"The New York Times Best Seller List"}])

    def test_get_download_link(self):
        data = json.loads(pkgutil.get_data(
            "tests.integrate",
            "files/overdrive/checkout_response_locked_in_format.json"))
        url = OverdriveAPI.get_download_link(
            data, "ebook-epub-adobe", "http://foo.com/")
        eq_("http://patron.api.overdrive.com/v1/patrons/me/checkouts/76C1B7D0-17F4-4C05-8397-C66C17411584/formats/ebook-epub-adobe/downloadlink?errorpageurl=http://foo.com/", url)
        eq_(None, OverdriveAPI.get_download_link(
            data, "no-such-format", "http://foo.com/"))
