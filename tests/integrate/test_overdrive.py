from nose.tools import set_trace, eq_
import pkgutil
import json
from tests.db import DatabaseTest
from integration.overdrive import (
    OverdriveAPI,
    OverdriveRepresentationExtractor
)
from model import (
    DataSource,
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

    def test_circulation_info_with_holds(self):
        data = pkgutil.get_data(
            "tests.integrate",
            "files/overdrive/overdrive_availability_information_holds.json")
        raw = json.loads(data)
        circulation = OverdriveRepresentationExtractor.circulation_info(raw)
        eq_(0, circulation[LicensedWork.AVAILABLE])
        eq_(10, circulation[LicensedWork.HOLDS])

    def test_link(self):
        data = pkgutil.get_data(
            "tests.integration",
            "files/overdrive/overdrive_book_list.json")
        raw = json.loads(data)
        expect = OverdriveAPI.make_link_safe("http://api.overdrive.com/v1/collections/collection-id/products?limit=300&offset=0&lastupdatetime=2014-04-28%2009:25:09&sort=popularity:desc&formats=ebook-epub-open,ebook-epub-adobe,ebook-pdf-adobe,ebook-pdf-open")
        eq_(expect, OverdriveRepresentationExtractor.link(raw, "first"))
