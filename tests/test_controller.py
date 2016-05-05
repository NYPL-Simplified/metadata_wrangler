import os
import base64
import feedparser
from datetime import datetime, timedelta
from nose.tools import set_trace, eq_

from . import DatabaseTest
from core.model import Identifier
from core.util.problem_detail import ProblemDetail

from controller import CollectionController

class TestCollectionController(DatabaseTest):

    def setup(self):
        super(TestCollectionController, self).setup()
        from app import app
        self.app = app

        self.controller = CollectionController(self._db)
        self.collection = self._collection()
        self.valid_auth = 'Basic ' + base64.b64encode('abc:def')

        self.work1 = self._work(with_license_pool=True, with_open_access_download=True)
        self.work2 = self._work(with_license_pool=True, with_open_access_download=True)

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

    def test_updates_feed(self):
        identifier = self.work1.license_pools[0].identifier
        self.collection.catalog_identifier(self._db, identifier)

        with self.app.test_request_context('/',
                headers=dict(Authorization=self.valid_auth)):
            response = self.controller.updates_feed()
            # The collection's updates feed is returned.
            eq_(200, response.status_code)
            feed = feedparser.parse(response.get_data())
            eq_(feed['feed']['title'],"%s Updates" % self.collection.name)
            
            # The feed has the collection's catalog.
            eq_(1, len(feed['entries']))
            [entry] = feed['entries']
            eq_(self.work1.title, entry['title'])
            eq_(identifier.urn, entry['id'])

        # A time can be passed.
        time = datetime.utcnow()
        timestamp = time.strftime("%Y-%m-%dT%H:%M:%SZ")
        self.work1.coverage_records[0].timestamp = time - timedelta(days=1)
        with self.app.test_request_context('/?last_update_time=%s' % timestamp,
                headers=dict(Authorization=self.valid_auth)):
            response = self.controller.updates_feed()
            eq_(200, response.status_code)
            feed = feedparser.parse(response.get_data())
            eq_(feed['feed']['title'],"%s Updates" % self.collection.name)
            # The timestamp is included in the url.
            linkified_timestamp = time.strftime("%Y-%m-%d+%H:%M:%S").replace(":", "%3A")
            assert feed['feed']['id'].endswith(linkified_timestamp)
            # And only works updated since the timestamp are returned.
            eq_(0, len(feed['entries']))

        # Works updated since the timestamp are returned
        self.work1.coverage_records[0].timestamp = datetime.utcnow()
        with self.app.test_request_context('/?last_update_time=%s' % timestamp,
                headers=dict(Authorization=self.valid_auth)):
            response = self.controller.updates_feed()
            feed = feedparser.parse(response.get_data())
            eq_(1, len(feed['entries']))
            [entry] = feed['entries']
            eq_(self.work1.title, entry['title'])
            eq_(identifier.urn, entry['id'])

    def test_updates_feed_is_paginated(self):
        for work in [self.work1, self.work2]:
            self.collection.catalog_identifier(
                self._db, work.license_pools[0].identifier
            )
        with self.app.test_request_context('/?size=1',
                headers=dict(Authorization=self.valid_auth)):
            response = self.controller.updates_feed()
            links = feedparser.parse(response.get_data())['feed']['links']
            assert any([link['rel'] == 'next' for link in links])
            assert not any([link['rel'] == 'previous' for link in links])
            assert not any([link['rel'] == 'first' for l in links])

        with self.app.test_request_context('/?size=1&after=1',
                headers=dict(Authorization=self.valid_auth)):
            response = self.controller.updates_feed()
            links = feedparser.parse(response.get_data())['feed']['links']
            assert any([link['rel'] == 'previous' for link in links])
            assert any([link['rel'] == 'first' for link in links])
            assert not any([link['rel'] == 'next'for link in links])

    def test_remove_items(self):
        invalid_urn = "FAKE AS I WANNA BE"
        catalogued_id = self._identifier()
        uncatalogued_id = self._identifier()
        self.collection.catalog_identifier(self._db, catalogued_id)

        with self.app.test_request_context(
                '/?urn=%s&urn=%s' % (catalogued_id.urn, uncatalogued_id.urn),
                headers=dict(Authorization=self.valid_auth)):
            # The uncatalogued identifier doesn't raise or return an error.
            response = self.controller.remove_items()
            eq_(200, response.status_code)
            entries = feedparser.parse(response.get_data())['entries']
            eq_(2, len(entries))

            catalogued = filter(lambda e: e['id']==catalogued_id.urn, entries)[0]
            uncatalogued = filter(lambda e: e['id']==uncatalogued_id.urn, entries)[0]
            eq_(200, int(catalogued['simplified_status_code']))
            eq_("Successfully removed", catalogued['simplified_message'])
            eq_(404, int(uncatalogued['simplified_status_code']))
            eq_("Not in collection catalog", uncatalogued['simplified_message'])

            # The catalogued identifier isn't in the catalog.
            assert catalogued_id not in self.collection.catalog
            # But it's still in the database.
            eq_(catalogued_id, self._db.query(Identifier).filter_by(
                id=catalogued_id.id).one())

        self.collection.catalog_identifier(self._db, catalogued_id)
        with self.app.test_request_context(
                '/?urn=%s&urn=%s' % (invalid_urn, catalogued_id.urn),
                headers=dict(Authorization=self.valid_auth)):
            response = self.controller.remove_items()
            eq_(200, int(response.status_code))
            entries = feedparser.parse(response.get_data())['entries']
            eq_(2, len(entries))

            invalid = filter(lambda e: e['id']==invalid_urn, entries)[0]
            eq_(400, int(invalid['simplified_status_code']))
            eq_("Could not parse identifier.", invalid['simplified_message'])
            # The catalogued identifier is still removed.
            assert catalogued_id not in self.collection.catalog
