# encoding: utf-8
"""Test the Flask app for the circulation server."""

from nose.tools import (
    eq_,
    set_trace,
)
from tests.db import (
    DatabaseTest,
)

from flask import url_for
import circulation

class CirculationTest(DatabaseTest):
    # TODO: The language-based tests assumes that the default sitewide
    # language is English.

    def setup(self):
        super(CirculationTest, self).setup()
        circulation.app.config['TESTING'] = True
        circulation.db = self._db
        self.circulation = circulation
        self.app = circulation.app
        self.client = circulation.app.test_client()

        # Create two English books and a French book.
        self.english_1 = self._work(
            "Quite British", "John Bull", "Fiction",
            "eng", True
        )
        self.english_2 = self._work(
            "Totally American", "Uncle Sam", "Nonfiction", "eng", True
        )
        self.french_1 = self._work(
            u"Très Français", "Marianne", "Nonfiction", "fre", True
        )


class TestNavigationFeed(CirculationTest):

    def test_root_redirects_to_navigation_feed(self):
        response = self.client.get('/')
        eq_(302, response.status_code)
        assert response.headers['Location'].endswith('/lanes/')

    def test_lane_without_language_preference_uses_default_language(self):
        response = self.client.get('/lanes/Nonfiction')
        assert "Totally American" in response.data
        assert "Quite British" not in response.data # Wrong lane
        assert u"Très Français" not in response.data # Wrong language

        # Now change the default language.
        old_default = circulation.DEFAULT_LANGUAGES
        circulation.DEFAULT_LANGUAGES = ["fre"]
        response = self.client.get('/lanes/Nonfiction')
        assert "Totally American" not in response.data
        assert u"Très Français".encode("utf8") in response.data
        circulation.DEFAULT_LANGUAGES = old_default

    def test_lane_with_language_preference(self):
        
        response = self.client.get(
            '/lanes/Nonfiction', headers={"Accept-Language": "fr"})
        assert "Totally American" not in response.data
        assert u"Très Français".encode("utf8") in response.data

        response = self.client.get(
            '/lanes/Nonfiction', headers={"Accept-Language": "fr,en-us"})
        assert "Totally American" in response.data
        assert u"Très Français".encode("utf8") in response.data


class TestCheckout(CirculationTest):

    def setup(self):
        super(TestCheckout, self).setup()
        pool = self.english_1.license_pools[0]
        work_record = pool.work_record()
        data_source = work_record.data_source
        identifier = work_record.primary_identifier

        with self.app.test_request_context('/'):
            self.url = url_for(
                'checkout', data_source=data_source.name,
                identifier=identifier.identifier)
    
    def test_checkout_requires_authentication(self):

        response = self.client.get(self.url)
        eq_(401, response.status_code)
