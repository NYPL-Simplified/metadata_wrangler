from nose.tools import set_trace
from datetime import datetime
from flask import request, make_response
import base64
import json
import logging
import urllib

from core.app_server import (
    cdn_url_for,
    feed_response,
    load_pagination_from_request,
    URNLookupController as CoreURNLookupController,
)
from core.model import (
    Collection,
    CoverageRecord,
    DataSource,
    Identifier,
    IntegrationClient,
    create,
    get_one,
)
from core.opds import (
    AcquisitionFeed,
    VerboseAnnotator,
)
from core.util.opds_writer import OPDSMessage
from core.util.problem_detail import ProblemDetail
from core.problem_details import (
    INVALID_CREDENTIALS,
    INVALID_INPUT,
    INVALID_URN,
)

from canonicalize import AuthorNameCanonicalizer

HTTP_OK = 200
HTTP_CREATED = 201
HTTP_ACCEPTED = 202
HTTP_UNAUTHORIZED = 401
HTTP_NOT_FOUND = 404
HTTP_INTERNAL_SERVER_ERROR = 500


def authenticated_client_from_request(_db, required=True):
    header = request.authorization
    if header:
        key, secret = header.username, header.password
        client = IntegrationClient.authenticate(_db, key, secret)
        if client:
            return client
    if not required and not header:
        # In the case that authentication is not required
        # (i.e. URN lookup) return None instead of an error.
        return None
    return INVALID_CREDENTIALS


class CanonicalizationController(object):

    log = logging.getLogger("Canonicalization Controller")

    def __init__(self, _db):
        self._db = _db
        self.canonicalizer = AuthorNameCanonicalizer(self._db)

    def canonicalize_author_name(self):
        urn = request.args.get('urn')
        display_name = request.args.get('display_name')
        if urn:
            identifier, is_new = Identifier.parse_urn(self._db, urn, False)
            if not isinstance(identifier, Identifier):
                return INVALID_URN
        else:
            identifier = None

        author_name = self.canonicalizer.canonicalize_author_name(
            identifier, display_name
        )
        self.log.info(
            "Incoming display name/identifier: %r/%s. Canonicalizer said: %s",
            display_name, identifier, author_name
        )

        if not author_name:
            return make_response("", HTTP_NOT_FOUND)
        return make_response(author_name, HTTP_OK, {"Content-Type": "text/plain"})


class CatalogController(object):
    """A controller to manage a Collection's catalog"""

    def __init__(self, _db):
        self._db = _db

    def updates_feed(self, collection_details):
        client = authenticated_client_from_request(self._db)
        if isinstance(client, ProblemDetail):
            return client

        collection, ignore = Collection.from_metadata_identifier(
            self._db, collection_details
        )

        last_update_time = request.args.get('last_update_time', None)
        if last_update_time:
            last_update_time = datetime.strptime(last_update_time, "%Y-%m-%dT%H:%M:%SZ")
        updated_works = collection.works_updated_since(self._db, last_update_time)

        pagination = load_pagination_from_request()
        works = pagination.apply(updated_works).all()
        title = "%s Collection Updates for %s" % (collection.protocol, client.url)
        def update_url(time=last_update_time, page=None):
            kw = dict(
                _external=True,
                collection_metadata_identifier=collection_details
            )
            if time:
                kw.update({'last_update_time' : last_update_time})
            if page:
                kw.update(page.items())
            return cdn_url_for("updates", **kw)
        update_feed = AcquisitionFeed(
            self._db, title, update_url(), works, VerboseAnnotator
        )

        if len(updated_works.all()) > pagination.size + pagination.offset:
            update_feed.add_link_to_feed(
                update_feed.feed, rel="next", 
                href=update_url(page=pagination.next_page)
            )
        if pagination.offset > 0:
            update_feed.add_link_to_feed(
                update_feed.feed, rel="first", 
                href=update_url(page=pagination.first_page)
            )
        previous_page = pagination.previous_page
        if previous_page:
            update_feed.add_link_to_feed(
                update_feed.feed, rel="previous", 
                href=update_url(page=previous_page)
            )

        return feed_response(update_feed)

    def remove_items(self, collection_details):
        """Removes identifiers from a collection's catalog"""
        client = authenticated_client_from_request(self._db)
        if isinstance(client, ProblemDetail):
            return client

        collection, ignore = Collection.from_metadata_identifier(
            self._db, collection_details
        )

        urns = request.args.getlist('urn')
        messages = []
        for urn in urns:
            message = None
            identifier = None
            try:
                identifier, ignore = Identifier.parse_urn(self._db, urn)
            except Exception as e:
                identifier = None
            if not identifier:
                message = OPDSMessage(
                    urn, INVALID_URN.status_code, INVALID_URN.detail
                )
            else:
                if identifier in collection.catalog:
                    collection.catalog.remove(identifier)
                    message = OPDSMessage(
                        urn, HTTP_OK, "Successfully removed"
                    )
                else:
                    message = OPDSMessage(
                        urn, HTTP_NOT_FOUND, "Not in catalog"
                    )
            if message:
                messages.append(message)

        title = "%s Catalog Item Removal for %s" % (collection.protocol, client.url)
        url = cdn_url_for("remove", collection_metadata_identifier=collection.name, urn=urns)
        removal_feed = AcquisitionFeed(
            self._db, title, url, [], VerboseAnnotator,
            precomposed_entries=messages
        )

        return feed_response(removal_feed)

    def update_client_url(self):
        """Updates the URL of a IntegrationClient"""
        client = authenticated_client_from_request(self._db)
        if isinstance(client, ProblemDetail):
            return client

        url = request.args.get('client_url')
        if not url:
            return INVALID_INPUT.detailed("No 'client_url' provided")

        client.url = IntegrationClient.normalize_url(urllib.unquote(url))

        return make_response("", HTTP_OK)


class URNLookupController(CoreURNLookupController):

    UNRESOLVABLE_IDENTIFIER = "I can't gather information about an identifier of this type."
    IDENTIFIER_REGISTERED = "You're the first one to ask about this identifier. I'll try to find out about it."
    WORKING_TO_RESOLVE_IDENTIFIER = "I'm working to locate a source for this identifier."
    SUCCESS_DID_NOT_RESULT_IN_PRESENTATION_READY_WORK = "Something's wrong. I have a record of covering this identifier but there's no presentation-ready work to show you."
    
    OPERATION = CoverageRecord.RESOLVE_IDENTIFIER_OPERATION
    NO_WORK_DONE_EXCEPTION = u'No work done yet'


    log = logging.getLogger("URN lookup controller")
    
    def presentation_ready_work_for(self, identifier):
        """Either return a presentation-ready work associated with the 
        given `identifier`, or return None.
        """
        pool = identifier.licensed_through
        if not pool:
            return None
        work = pool.work
        if not work or not work.presentation_ready:
            return None
        return work
    
    def can_resolve_identifier(self, identifier):
        """A chance to determine whether resolution should proceed."""
        # We can resolve any ISBN and any Overdrive ID.
        #
        # We can resolve any Gutenberg ID by looking it up in the open-access
        # content server.
        #
        # We can attempt to resolve URIs by looking them up in the
        # open-access content server, though there's no guarantee
        # it will work.        
        if identifier.type in (
                Identifier.ISBN, Identifier.OVERDRIVE_ID,
                Identifier.GUTENBERG_ID, Identifier.URI
        ):
            return True
        
        # We can resolve any identifier that's associated with a
        # presentation-ready work, since the resolution has already
        # been done--no need to speculate about how.
        work = self.presentation_ready_work_for(identifier)
        if work is None:
            return False
        return True
  
    def process_urn(self, urn, collection_details=None, **kwargs):
        """Turn a URN into a Work suitable for use in an OPDS feed.
        """
        try:
            identifier, is_new = Identifier.parse_urn(self._db, urn)
        except ValueError, e:
            identifier = None

        if not identifier:
            # Not a well-formed URN.
            return self.add_message(urn, 400, INVALID_URN.detail)

        if not self.can_resolve_identifier(identifier):
            return self.add_message(urn, HTTP_NOT_FOUND, self.UNRESOLVABLE_IDENTIFIER)

        # We are at least willing to try to resolve this Identifier.
        # If a Collection was provided by an authenticated IntegrationClient,
        # this Identifier is part of the Collection's catalog.
        client = authenticated_client_from_request(self._db, required=False)
        if client and collection_details:
            collection, ignore = Collection.from_metadata_identifier(
                self._db, collection_details
            )
            collection.catalog_identifier(self._db, identifier)

        if identifier.type == Identifier.ISBN:
            # ISBNs are handled specially.
            return self.make_opds_entry_from_metadata_lookups(identifier)

        # All other identifiers need to be associated with a
        # presentation-ready Work for the lookup to succeed. If there
        # isn't one, we need to register it as unresolved.
        work = self.presentation_ready_work_for(identifier)
        if work:
            # The work has been done.
            return self.add_work(identifier, work)

        # Work remains to be done.
        return self.register_identifier_as_unresolved(urn, identifier)

    def register_identifier_as_unresolved(self, urn, identifier):
        # This identifier could have a presentation-ready Work
        # associated with it, but it doesn't. We need to make sure the
        # work gets done eventually by creating a CoverageRecord
        # representing the work that needs to be done.
        source = DataSource.lookup(self._db, DataSource.INTERNAL_PROCESSING)
        
        record = CoverageRecord.lookup(identifier, source, self.OPERATION)
        is_new = False
        if not record:
            # There is no existing CoverageRecord for this Identifier.
            # Create one, but put it in a state of transient failure
            # to represent the fact that work needs to be done.
            record, is_new = CoverageRecord.add_for(
                identifier, source, self.OPERATION,
                status=CoverageRecord.TRANSIENT_FAILURE
            )
            record.exception = self.NO_WORK_DONE_EXCEPTION

        if is_new:
            # The CoverageRecord was just created. Tell the client to
            # come back later.
            return self.add_message(urn, HTTP_CREATED, self.IDENTIFIER_REGISTERED)
        else:
            # There is a pending attempt to resolve this identifier.
            # Tell the client we're working on it, or if the
            # pending attempt resulted in an exception,
            # tell the client about the exception.
            message = record.exception
            if not message or message == self.NO_WORK_DONE_EXCEPTION:
                message = self.WORKING_TO_RESOLVE_IDENTIFIER
            status = HTTP_ACCEPTED
            if record.status == record.PERSISTENT_FAILURE:
                # Apparently we just can't provide coverage of this
                # identifier.
                status = HTTP_INTERNAL_SERVER_ERROR
            elif record.status == record.SUCCESS:
                # This shouldn't happen, since success in providing
                # this sort of coverage means creating a presentation
                # ready work. Something weird is going on.
                status = HTTP_INTERNAL_SERVER_ERROR
                message = self.SUCCESS_DID_NOT_RESULT_IN_PRESENTATION_READY_WORK
            return self.add_message(urn, status, message)

    def make_opds_entry_from_metadata_lookups(self, identifier):
        """This identifier cannot be turned into a presentation-ready Work,
        but maybe we can make an OPDS entry based on metadata lookups.
        """

        # We can only create an OPDS entry if all the lookups have
        # in fact been done.
        metadata_sources = DataSource.metadata_sources_for(
            self._db, identifier
        )
        q = self._db.query(CoverageRecord).filter(
                CoverageRecord.identifier==identifier
        ).filter(
            CoverageRecord.data_source_id.in_(
                [x.id for x in metadata_sources]
            )
        )

        coverage_records = q.all()
        unaccounted_for = set(metadata_sources)
        for r in coverage_records:
            if r.data_source in unaccounted_for:
                unaccounted_for.remove(r.data_source)

        if unaccounted_for:
            # At least one metadata lookup has not successfully
            # completed.
            names = [x.name for x in unaccounted_for]
            self.log.info(
                "Cannot build metadata-based OPDS feed for %r: missing coverage records for %s",
                identifier,
                ", ".join(names)
            )
            return self.register_identifier_as_unresolved(
                identifier.urn, identifier
            )
        else:
            # All metadata lookups have completed. Create that OPDS
            # entry!
            entry = identifier.opds_entry()

        if entry is None:
            # We can't do lookups on an identifier of this type, so
            # the best thing to do is to treat this identifier as a
            # 404 error.
            return self.add_message(
                identifier.urn, HTTP_NOT_FOUND, self.UNRECOGNIZED_IDENTIFIER
            )

        # We made it!
        return self.add_entry(entry)

    def post_lookup_hook(self):
        """Run after looking up a number of Identifiers.

        We commit the database session because new Identifier and/or
        CoverageRecord objects may have been created during the
        lookup process.
        """
        self._db.commit()
