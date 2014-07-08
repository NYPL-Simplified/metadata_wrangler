# encoding: utf-8
"""Test the Flask app for the circulation server."""

from nose.tools import (
    eq_,
    set_trace,
)
from tests.db import (
    DatabaseTest,
)

import circulation

class CirculationTest(DatabaseTest):
    # TODO: The language-based tests assumes that the default sitewide
    # language is English.

    def setup(self):
        super(CirculationTest, self).setup()
        circulation.app.config['TESTING'] = True
        circulation.db = self._db
        self.app = circulation.app.test_client()

        # Create two English books and a French book.
        english_1 = self._work(
            "Quite British", "John Bull", "Fiction",
            "eng", True
        )
        english_2 = self._work(
            "Totally American", "Uncle Sam", "Nonfiction", "eng", True
        )
        french_1 = self._work(
            u"Très Français", "Marianne", "Nonfiction", "eng", True
        )


class TestNavigationFeed(CirculationTest):

    def test_root_redirects_to_navigation_feed(self):
        response = self.app.get('/')
        eq_(302, response.status_code)
        assert response.headers['Location'].endswith('/lanes/')

    def test_lane_without_language_preference_uses_default_language(self):
        response = self.app.get('/lanes/Nonfiction')
        assert "Totally American" in response.data
        assert "Quite British" not in response.data # Wrong lane
        assert u"Très Français" not in response.data # Wrong language

        # Now change the default language.
        old_default = circulation.DEFAULT_LANGUAGES
        circulation.DEFAULT_LANGUAGES = ["fre"]
        response = self.app.get('/lanes/Nonfiction')
        assert "Totally American" not in response.data
        assert u"Très Français" in response.data
        circulation.DEFAULT_LANGUAGES = old_default

    def test_lane_with_language_preference(self):
        
        response = self.app.get(
            '/lanes/Nonfiction', headers={"Accept-Language": "fr"})
        assert "Totally American" not in response.data
        assert u"Très Français" in response.data

        response = self.app.get(
            '/lanes/Nonfiction', headers={"Accept-Language": "fr,en-us"})
        assert "Totally American" in response.data
        assert u"Très Français" in response.data
