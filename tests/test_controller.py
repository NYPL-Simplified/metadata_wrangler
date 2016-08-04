import os
import base64
import feedparser
from datetime import datetime, timedelta
from nose.tools import set_trace, eq_

from . import DatabaseTest
from core.model import (
    Identifier,
    UnresolvedIdentifier,
)
from core.util.problem_detail import ProblemDetail

from controller import (
    CollectionController,
    URNLookupController,
)

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
        for record in self.work1.coverage_records:
            # Set back the clock on all of work1's time records
            record.timestamp = time - timedelta(days=1)
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


class TestURNLookupController(DatabaseTest):

    def setup(self):
        super(TestURNLookupController, self).setup()
        self.controller = URNLookupController(self._db)

    def assert_one_message(self, urn, code, message):
        """Assert that the given message is the only one in
        messages_by_urn.
        """
        [(u, (c, m))] = self.controller.messages_by_urn.items()
        eq_(u, urn)
        eq_(c, code)
        eq_(m, message)
        eq_([], self.controller.works)
        eq_([], self.controller.precomposed_entries)

    def test_process_urn_initial_registration(self):
        urn = Identifier.URN_SCHEME_PREFIX + "Overdrive ID/nosuchidentifier"
        self.controller.process_urn(urn)
        self.assert_one_message(
            urn, 201, URNLookupController.IDENTIFIER_REGISTERED
        )

        # The Identifier has been created and an UnresolvedIdentifier
        # associated with it.
        [identifier] = self._db.query(Identifier).filter(
            Identifier.type==Identifier.OVERDRIVE_ID
        ).all()
        eq_("nosuchidentifier", identifier.identifier)
        unresolved = identifier.unresolved_identifier
        eq_(202, unresolved.status)

    def test_process_urn_pending_resolve_attempt(self):
        identifier = self._identifier(Identifier.GUTENBERG_ID)
        unresolved, is_new = UnresolvedIdentifier.register(self._db, identifier)
        self.controller.process_urn(identifier.urn)
        eq_(1, len(self.controller.messages_by_urn.keys()))
        self.assert_one_message(
            identifier.urn, 202,
            URNLookupController.WORKING_TO_RESOLVE_IDENTIFIER
        )

    def test_process_urn_exception_during_resolve_attempt(self):
        identifier = self._identifier(Identifier.GUTENBERG_ID)
        unresolved, is_new = UnresolvedIdentifier.register(self._db, identifier)
        unresolved.status = 500
        unresolved.exception = "foo"
        self.controller.process_urn(identifier.urn)
        eq_(1, len(self.controller.messages_by_urn.keys()))
        self.assert_one_message(
            identifier.urn, 500, "foo"
        )

    def test_process_urn_unresolvable_type(self):
        # We can't resolve a 3M identifier because we don't have the
        # appropriate access to the bibliographic API.
        identifier = self._identifier(Identifier.THREEM_ID)
        self.controller.process_urn(identifier.urn)
        eq_(1, len(self.controller.messages_by_urn.keys()))
        self.assert_one_message(
            identifier.urn, 404, self.controller.UNRESOLVABLE_IDENTIFIER
        )

    def test_presentation_ready_work_overrides_unresolveable_type(self):
        # If there is a presentation-ready Work associated
        # with the identifier, turns out we can resolve it even if the
        # type would otherwise not be resolvable.
        edition, pool = self._edition(
            identifier_type=Identifier.THREEM_ID, with_license_pool=True
        )
        pool.open_access = False
        work, is_new = pool.calculate_work()
        work.presentation_ready = True
        identifier = edition.primary_identifier
        self.controller.process_urn(identifier.urn)
        eq_([(identifier, work)], self.controller.works)
        
    def test_process_urn_with_collection(self):
        collection = self._collection()
        i1 = self._identifier()
        i2 = self._identifier()

        eq_([], collection.catalog)
        self.controller.process_urn(i1.urn, collection=collection)
        eq_(1, len(collection.catalog))
        eq_([i1], collection.catalog)

        # Adds new identifiers to an existing catalog
        self.controller.process_urn(i2.urn, collection=collection)
        eq_(2, len(collection.catalog))
        eq_([i1, i2], collection.catalog)

        # Does not duplicate identifiers in the catalog
        self.controller.process_urn(i1.urn, collection=collection)
        eq_(2, len(collection.catalog))
        eq_([i1, i2], collection.catalog)

    def test_process_urn_isbn_not_ready(self):
        isbn, ignore = Identifier.for_foreign_id(
            self._db, Identifier.ISBN, self._isbn
        )
        self.controller.process_urn(isbn.urn)
        self.assert_one_message(
            isbn.urn, 201, self.controller.IDENTIFIER_REGISTERED
        )
        unresolved, is_new = UnresolvedIdentifier.register(self._db, isbn)
        eq_(False, is_new)

