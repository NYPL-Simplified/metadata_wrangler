from nose.tools import set_trace, eq_
import pkgutil
import json
from model import LicensedWork
from integration.overdrive import (
    OverdriveAPI,
    OverdriveRepresentationExtractor)

class TestOverdriveRepresentationExtractor(object):

    def test_availability_info(self):
        data = pkgutil.get_data(
            "tests.integration",
            "files/overdrive_book_list.json")
        raw = json.loads(data)
        availability = OverdriveRepresentationExtractor.availability_info(raw)
        for item in availability:
            for key in 'availability_link', 'id':
                assert key in item

    def test_circulation_info(self):
        data = pkgutil.get_data(
            "tests.integration",
            "files/overdrive_availability_information.json")
        raw = json.loads(data)
        circulation = OverdriveRepresentationExtractor.circulation_info(raw)
        for key in (LicensedWork.SOURCE_ID,
                    LicensedWork.OWNED,
                    LicensedWork.AVAILABLE,
                    LicensedWork.HOLDS,
                    LicensedWork.RESERVES):
            assert key in circulation

    def test_circulation_info_with_holds(self):
        data = pkgutil.get_data(
            "tests.integration",
            "files/overdrive_availability_information_holds.json")
        raw = json.loads(data)
        circulation = OverdriveRepresentationExtractor.circulation_info(raw)
        eq_(0, circulation[LicensedWork.AVAILABLE])
        eq_(10, circulation[LicensedWork.HOLDS])

    def test_link(self):
        data = pkgutil.get_data(
            "tests.integration",
            "files/overdrive_book_list.json")
        raw = json.loads(data)
        expect = OverdriveAPI.make_link_safe("http://api.overdrive.com/v1/collections/collection-id/products?limit=300&offset=0&lastupdatetime=2014-04-28%2009:25:09&sort=popularity:desc&formats=ebook-epub-open,ebook-epub-adobe,ebook-pdf-adobe,ebook-pdf-open")
        eq_(expect, OverdriveRepresentationExtractor.link(raw, "first"))
