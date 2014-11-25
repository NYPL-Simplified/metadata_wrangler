# encoding: utf-8
from nose.tools import (
    set_trace, eq_,
    assert_raises,
)
import datetime
import json
import pkgutil

from tests.db import DatabaseTest
from integration.bibliocommons import BibliocommonsAPI

class DummyBibliocommonsAPI(BibliocommonsAPI):

    def list_pages_for_user(self, user_id, max_age=None):
        for pagenum in range(1,4):
            data = pkgutil.get_data(
                "tests.integrate",
                "files/bibliocommons/list_of_user_lists_page%d.json" % pagenum)
            yield json.loads(data)


class TestBibliocommonsAPI(DatabaseTest):
    
    def setup(self):
        super(TestBibliocommonsAPI, self).setup()
        self.api = DummyBibliocommonsAPI(self._db)

    def test_list_data_for_user(self):

        all_lists = list(self.api.list_data_for_user("any user"))
        eq_(28, len(all_lists))
        first_list = all_lists[0]

        # Basic list data is present.
        eq_('331352747', first_list['id'])

        # Updated and created dates have been converted to datetimes.
        eq_(datetime.datetime(2014, 9, 30, 20, 55, 13), first_list['updated'])
        eq_(datetime.datetime(2014, 9, 30, 20, 30, 25), first_list['created'])
