import os
import base64
import feedparser
from nose.tools import set_trace, eq_

from . import DatabaseTest
from ..core.model import DataSource
from ..core.util.problem_detail import ProblemDetail

from ..controller import CollectionController

class TestCollectionController(DatabaseTest):

    def setup(self):
        super(TestCollectionController, self).setup()
        from ..app import app
        self.app = app

        self.controller = CollectionController(self._db)
        self.collection = self._collection()
        self.valid_auth = 'Basic ' + base64.b64encode('abc:def')

    def test_authenticated_collection_required(self):
        # Returns collection if authentication is valid.
        with self.app.test_request_context('/',
                headers=dict(Authorization=self.valid_auth)):
            result = self.controller.authenticated_collection_from_request()
            eq_(result, self.collection)
        
        # Returns error if authentication is invalid.
        invalid_auth = 'Basic ' + base64.b64encode('abc:defg')
        with self.app.test_request_context('/',
                headers=dict(Authorization=invalid_auth)):
            result = self.controller.authenticated_collection_from_request()
            eq_(True, isinstance(result, ProblemDetail))
            eq_(401, result.status_code)

        # Returns errors without authentication.
        with self.app.test_request_context('/'):
            result = self.controller.authenticated_collection_from_request()
            eq_(True, isinstance(result, ProblemDetail))

    def test_authenticated_collection_optional(self):
        # Returns collection of authentication is valid.
        with self.app.test_request_context('/',
                headers=dict(Authorization=self.valid_auth)):
            result = self.controller.authenticated_collection_from_request(required=False)
            eq_(result, self.collection)
        
        # Returns error if attempted authentication is invalid.
        invalid_auth = 'Basic ' + base64.b64encode('abc:defg')
        with self.app.test_request_context('/',
                headers=dict(Authorization=invalid_auth)):
            result = self.controller.authenticated_collection_from_request(required=False)
            eq_(True, isinstance(result, ProblemDetail))
            eq_(401, result.status_code)

        # Returns none if no authentication.
        with self.app.test_request_context('/'):
            result = self.controller.authenticated_collection_from_request(required=False)
            eq_(None, result)

    def test_collection_updates(self):
        w1 = self._work(with_license_pool=True, with_open_access_download=True)
        identifier = w1.license_pools[0].identifier
        self.collection.catalog_identifier(self._db, identifier)

        # Collection hasn't checked its updates at all
        eq_(None, self.collection.last_checked)

        with self.app.test_request_context('/',
                headers=dict(Authorization=self.valid_auth)):
            response = self.controller.updates_feed()
            eq_(200, response.status_code)
            feed = feedparser.parse(response.get_data())
            eq_(feed['feed']['title'],"%s Updates" % self.collection.name)
            
            eq_(1, len(feed['entries']))
            [entry] = feed['entries']
            eq_(w1.title, entry['title'])
            eq_(identifier.urn, entry['id'])

        # The collection's last check timestamp has be set
        assert self.collection.last_checked
        previous_check = self.collection.last_checked

        # Add another work.
        w2 = self._work(with_license_pool=True, with_open_access_download=True)
        w2_identifier = w2.license_pools[0].identifier
        self.collection.catalog_identifier(self._db, w2_identifier)
        with self.app.test_request_context('/',
                headers=dict(Authorization=self.valid_auth)):
            response = self.controller.updates_feed()
            eq_(200, response.status_code)
            feed = feedparser.parse(response.get_data())
            # Only the second work is in the feed.
            eq_(1, len(feed['entries']))
            [entry] = feed['entries']
            eq_(w2.title, entry['title'])
            eq_(w2_identifier.urn, entry['id'])
