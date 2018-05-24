from nose.tools import set_trace
from datetime import datetime
from flask import request, make_response
from flask_babel import lazy_gettext as _
from lxml import etree
from sqlalchemy import (
    and_,
    not_,
)
from sqlalchemy.orm import joinedload
from sqlalchemy.sql.expression import and_
from Crypto.PublicKey import RSA
from Crypto.Cipher import PKCS1_OAEP
import base64
import feedparser
import json
import jwt
import logging
import urllib
import urlparse

from core.app_server import (
    cdn_url_for,
    feed_response,
    load_pagination_from_request,
    Pagination,
    URNLookupController as CoreURNLookupController,
)
from core.config import Configuration
from core.model import (
    Collection,
    collections_identifiers,
    ConfigurationSetting,
    Contributor,
    CoverageRecord,
    DataSource,
    Edition,
    Hyperlink,
    Identifier,
    IntegrationClient,
    LicensePool,
    PresentationCalculationPolicy,
    Representation,
    Work,
    create,
    get_one,
    get_one_or_create,
)
from core.metadata_layer import (
    Metadata,
    ContributorData,
    IdentifierData,
    LinkData,
    ReplacementPolicy,
)
from core.opds import (
    AcquisitionFeed,
    LookupAcquisitionFeed,
    VerboseAnnotator,
)
from core.util import fast_query_count
from core.util.authentication_for_opds import AuthenticationForOPDSDocument
from core.util.http import HTTP
from core.util.opds_writer import OPDSMessage
from core.util.problem_detail import ProblemDetail

from coverage import (
    IdentifierResolutionCoverageProvider,
    IdentifierResolutionRegistrar,
)
from canonicalize import AuthorNameCanonicalizer
from integration_client import IntegrationClientCoverImageCoverageProvider
from problem_details import *

HTTP_OK = 200
HTTP_CREATED = 201
HTTP_ACCEPTED = 202
HTTP_UNAUTHORIZED = 401
HTTP_NOT_FOUND = 404
HTTP_INTERNAL_SERVER_ERROR = 500

OPDS_2_MEDIA_TYPE = 'application/opds+json'


def authenticated_client_from_request(_db, required=True):
    header = request.headers.get('Authorization')
    if header and 'bearer' in header.lower():
        shared_secret = base64.b64decode(header.split(' ')[1])
        client = IntegrationClient.authenticate(_db, shared_secret)
        if client:
            return client
    if not required and not header:
        # In the case that authentication is not required
        # (i.e. URN lookup) return None instead of an error.
        return None
    return INVALID_CREDENTIALS


def collection_from_details(_db, client, collection_details):
    if not (client and isinstance(client, IntegrationClient)):
        return None

    if collection_details:
        # A DataSource may be sent for collections with the
        # ExternalIntegration.OPDS_IMPORT protocol.
        data_source_name = request.args.get('data_source')
        if data_source_name:
            data_source_name = urllib.unquote(data_source_name)

        collection, ignore = Collection.from_metadata_identifier(
            _db, collection_details, data_source=data_source_name
        )
        return collection
    return None


class IndexController(object):

    def __init__(self, _db):
        self._db = _db

    def opds_catalog(self):
        url = ConfigurationSetting.sitewide(self._db, Configuration.BASE_URL_KEY).value
        catalog = dict(
            id=url,
            title='Library Simplified Metadata Wrangler',
        )

        catalog['links'] = [
            {
                "rel": "register",
                "href": "/register",
                "type": "application/opds+json;profile=https://librarysimplified.org/rel/profile/metadata-service",
                "title": "Register your OPDS server with this metadata service"
            },
            {
                "rel": "http://librarysimplified.org/rel/metadata/lookup",
                "href": "/lookup{?data_source,urn*}",
                "type": "application/atom+xml;profile=opds-catalog",
                "title": "Look up metadata about one or more specific items",
                "templated": "true"
            },
            {
                "rel": "http://opds-spec.org/sort/new",
                "href": "/{collection_metadata_identifier}/updates{?data_source}",
                "type": "application/atom+xml;profile=opds-catalog",
                "title": "Recent changes to one of your tracked collections.",
                "templated": "true"
            },
            {
                "rel": "http://librarysimplified.org/rel/metadata/collection-add",
                "href": "/{collection_metadata_identifier}/add{?data_source,urn*}",
                "title": "Add items to one of your tracked collections.",
                "templated": "true"
            },
            {
                "rel": "http://librarysimplified.org/rel/metadata/collection-remove",
                "href": "/{collection_metadata_identifier}/remove{?data_source,urn*}",
                "title": "Remove items from one of your tracked collections.",
                "templated": "true"
            },
            {
                "rel": "http://librarysimplified.org/rel/metadata/collection-metadata-needed",
                "href": "/{collection_metadata_identifier}/metadata_needed",
                "title": "Get items in your collection for which the metadata wrangler needs more information to process.",
                "templated": "true"
            },
            {
                "rel": "http://librarysimplified.org/rel/metadata/resolve-name",
                "href": "/canonical-author-name{?urn,display_name}",
                "type": "text/plain",
                "title": "Look up an author's canonical sort name",
                "templated": "true"
            }
        ]

        return make_response(
            json.dumps(catalog), HTTP_OK,
            {'Content-Type' : OPDS_2_MEDIA_TYPE }
        )


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


class ISBNEntryMixin(object):

    def make_opds_entry_from_metadata_lookups(self, identifier):
        """This identifier cannot be turned into a presentation-ready Work,
        but maybe we can make an OPDS entry based on metadata lookups.
        """

        # We can only create an OPDS entry if all the lookups have
        # in fact been done.
        metadata_sources = DataSource.metadata_sources_for(
            self._db, identifier
        )
        q = self._db.query(CoverageRecord)\
            .filter(CoverageRecord.identifier==identifier)\
            .filter(
                CoverageRecord.data_source_id.in_([x.id for x in metadata_sources]),
                CoverageRecord.status==CoverageRecord.SUCCESS
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
            return None

        # All metadata lookups have completed. Create that OPDS
        # entry!
        entry = identifier.opds_entry()
        if entry is None:
            # We can't do lookups on an identifier of this type, so
            # the best thing to do is to treat this identifier as a
            # 404 error.
            return OPDSMessage(
                identifier.urn, HTTP_NOT_FOUND, self.UNRECOGNIZED_IDENTIFIER
            )

        # We made it!
        return entry


class CatalogController(ISBNEntryMixin):
    """A controller to manage a Collection's catalog"""

    log = logging.getLogger("Catalog Controller")

    # Setting a default updates feed size lower than the Pagination.DEFAULT_SIZE
    # of 50. Because the updates feed paginates works and isbns separately,
    # this not-quite-half should keep the feed at about the expected size
    # overall without impacting non-ISBN collections too much.
    UPDATES_SIZE = 35

    TIMESTAMP_FORMAT = "%Y-%m-%dT%H:%M:%SZ"

    def __init__(self, _db):
        self._db = _db

    @classmethod
    def collection_feed_url(cls, endpoint, collection, page=None,
        **param_kwargs
    ):
        kw = dict(
            _external=True,
            collection_metadata_identifier=collection.name
        )
        kw.update(param_kwargs)
        if page:
            kw.update(page.items())
        return cdn_url_for(endpoint, **kw)

    @classmethod
    def add_pagination_links_to_feed(cls, pagination, query, feed, endpoint,
        collection, **url_param_kwargs
    ):
        """Adds links for pagination to a given collection's feed."""
        def href_for(page):
            return cls.collection_feed_url(
                endpoint, collection, page=page, **url_param_kwargs
            )

        if fast_query_count(query) > (pagination.size + pagination.offset):
            feed.add_link_to_feed(
                feed.feed, rel="next", href=href_for(pagination.next_page)
            )

        if pagination.offset > 0:
            feed.add_link_to_feed(
                feed.feed, rel="first", href=href_for(pagination.first_page)
            )

        previous_page = pagination.previous_page
        if previous_page:
            feed.add_link_to_feed(
                feed.feed, rel="previous", href=href_for(previous_page)
            )

    def updates_feed(self, collection_details):
        client = authenticated_client_from_request(self._db)
        if isinstance(client, ProblemDetail):
            return client

        collection = collection_from_details(
            self._db, client, collection_details
        )

        last_update_time = request.args.get('last_update_time', None)
        if last_update_time:
            try:
                last_update_time = datetime.strptime(
                    last_update_time, self.TIMESTAMP_FORMAT
                )
            except ValueError, e:
                message = 'The timestamp "%s" is not in the expected format (%s)'
                return INVALID_INPUT.detailed(
                    message % (last_update_time, self.TIMESTAMP_FORMAT)
                )

        pagination = load_pagination_from_request(default_size=self.UPDATES_SIZE)

        precomposed_entries = []
        # Add entries for Works associated with the collection's catalog.
        updated_works = collection.works_updated_since(self._db, last_update_time)
        works = pagination.apply(updated_works).all()
        annotator = VerboseAnnotator()
        works_for_feed = []
        for work, licensepool, identifier, ignore1, ignore2 in works:
            entry = work.verbose_opds_entry or work.simple_opds_entry
            if entry:
                # A cached OPDS entry for this Work already
                # exists. annotate it with LicensePool and
                # Identifier-specific information. We have to do this
                # ourselves because we're asking LookupAcquisitionFeed
                # to treat these as precomposed entries, meaning
                # they must be complete as-is.
                entry = etree.fromstring(entry)
                annotator.annotate_work_entry(
                    work, licensepool, None, identifier, None, entry
                )
                precomposed_entries.append(entry)
            else:
                # There is no cached OPDS entry. We'll create one later.
                works_for_feed.append((work, identifier))

        # Add entries for ISBNs associated with the collection's catalog.
        isbns = collection.isbns_updated_since(self._db, last_update_time)
        isbns = pagination.apply(isbns).all()
        for isbn, _latest_timestamp in isbns:
            entry = self.make_opds_entry_from_metadata_lookups(isbn)
            if entry:
                precomposed_entries.append(entry)

        title = "%s Collection Updates for %s" % (collection.protocol, client.url)
        url_params = dict()
        if last_update_time:
            url_params = dict(
                last_update_time=last_update_time.strftime(
                    self.TIMESTAMP_FORMAT
                )
            )
        url = self.collection_feed_url('updates', collection, **url_params)

        update_feed = LookupAcquisitionFeed(
            self._db, title, url, works_for_feed, annotator,
            precomposed_entries=precomposed_entries
        )

        self.add_pagination_links_to_feed(
            pagination, updated_works, update_feed, 'updates', collection,
            **url_params
        )

        return feed_response(update_feed)

    def add_items(self, collection_details):
        """Adds identifiers to a Collection's catalog"""
        client = authenticated_client_from_request(self._db)
        if isinstance(client, ProblemDetail):
            return client

        collection = collection_from_details(
            self._db, client, collection_details
        )
        urns = request.args.getlist('urn')
        messages = []
        identifiers_by_urn, failures = Identifier.parse_urns(self._db, urns)

        for urn in failures:
            message = OPDSMessage(
                urn, INVALID_URN.status_code, INVALID_URN.detail
            )
            messages.append(message)

        # Find the subset of incoming identifiers that are already
        # in the catalog.
        already_in_catalog, ignore = self._in_catalog_subset(
            collection, identifiers_by_urn
        )

        # Everything else needs to be added to the catalog.
        needs_to_be_added = [
            x for x in identifiers_by_urn.values()
            if x.id not in already_in_catalog
        ]
        collection.catalog_identifiers(needs_to_be_added)

        for urn, identifier in identifiers_by_urn.items():
            if identifier.id in already_in_catalog:
                status = HTTP_OK
                description = "Already in catalog"
            else:
                status = HTTP_CREATED
                description = "Successfully added"

            messages.append(OPDSMessage(urn, status, description))

        title = "%s Catalog Item Additions for %s" % (collection.protocol, client.url)
        url = self.collection_feed_url('add', collection, urn=urns)
        addition_feed = AcquisitionFeed(
            self._db, title, url, [], VerboseAnnotator,
            precomposed_entries=messages
        )

        return feed_response(addition_feed)

    def add_with_metadata(self, collection_details):
        """Adds identifiers with their metadata to a Collection's catalog"""
        client = authenticated_client_from_request(self._db)
        if isinstance(client, ProblemDetail):
            return client

        collection = collection_from_details(
            self._db, client, collection_details
        )

        data_source = DataSource.lookup(
            self._db, collection.name, autocreate=True
        )

        messages = []

        feed = feedparser.parse(request.data)
        entries = feed.get("entries", [])
        entries_by_urn = { entry.get('id') : entry for entry in entries }

        identifiers_by_urn, invalid_urns = Identifier.parse_urns(
            self._db, entries_by_urn.keys()
        )

        messages = list()

        for urn in invalid_urns:
            messages.append(OPDSMessage(
                urn, INVALID_URN.status_code, INVALID_URN.detail
            ))


        for urn, identifier in identifiers_by_urn.items():
            entry = entries_by_urn[urn]
            status = HTTP_OK
            description = "Already in catalog"

            if identifier not in collection.catalog:
                collection.catalog_identifier(identifier)
                status = HTTP_CREATED
                description = "Successfully added"

            message = OPDSMessage(urn, status, description)

            # Get a cover if it exists.
            image_types = set([Hyperlink.IMAGE, Hyperlink.THUMBNAIL_IMAGE])
            images = [l for l in entry.get("links", []) if l.get("rel") in image_types]
            links = [LinkData(image.get("rel"), image.get("href")) for image in images]

            # Create an edition to hold the title and author. LicensePool.calculate_work
            # refuses to create a Work when there's no title, and if we have a title, author
            # and language we can attempt to look up the edition in OCLC.
            title = entry.get("title") or "Unknown Title"
            author = ContributorData(
                sort_name=(entry.get("author") or Edition.UNKNOWN_AUTHOR),
                roles=[Contributor.PRIMARY_AUTHOR_ROLE]
            )
            language = entry.get("dcterms_language")

            presentation = PresentationCalculationPolicy(
                choose_edition=False,
                set_edition_metadata=False,
                classify=False,
                choose_summary=False,
                calculate_quality=False,
                choose_cover=False,
                regenerate_opds_entries=False,
            )
            replace = ReplacementPolicy(presentation_calculation_policy=presentation)
            metadata = Metadata(
                data_source,
                primary_identifier=IdentifierData(identifier.type, identifier.identifier),
                title=title,
                language=language,
                contributors=[author],
                links=links,
            )

            edition, ignore = metadata.edition(self._db)
            metadata.apply(edition, collection, replace=replace)

            messages.append(message)

        title = "%s Catalog Item Additions for %s" % (collection.protocol, client.url)
        url = self.collection_feed_url("add_with_metadata", collection)
        addition_feed = AcquisitionFeed(
            self._db, title, url, [], VerboseAnnotator,
            precomposed_entries=messages
        )

        return feed_response(addition_feed)

    def metadata_needed_for(self, collection_details):
        """Returns identifiers in the collection that could benefit from
        distributor metadata on the circulation manager.
        """
        client = authenticated_client_from_request(self._db)
        if isinstance(client, ProblemDetail):
            return client

        collection = collection_from_details(
            self._db, client, collection_details
        )

        resolver = IdentifierResolutionCoverageProvider
        unresolved_identifiers = collection.unresolved_catalog(
            self._db, resolver.DATA_SOURCE_NAME, resolver.OPERATION
        )

        # Omit identifiers that currently have metadata pending for
        # the IntegrationClientCoverImageCoverageProvider.
        data_source = DataSource.lookup(
            self._db, collection.name, autocreate=True
        )
        is_awaiting_metadata = self._db.query(
            CoverageRecord.id, CoverageRecord.identifier_id
        ).filter(
            CoverageRecord.data_source_id==data_source.id,
            CoverageRecord.status==CoverageRecord.REGISTERED,
            CoverageRecord.operation==IntegrationClientCoverImageCoverageProvider.OPERATION,
        ).subquery()

        unresolved_identifiers = unresolved_identifiers.outerjoin(
            is_awaiting_metadata,
            Identifier.id==is_awaiting_metadata.c.identifier_id
        ).filter(is_awaiting_metadata.c.id==None)

        # Add a message for each unresolved identifier
        pagination = load_pagination_from_request(default_size=25)
        feed_identifiers = pagination.apply(unresolved_identifiers).all()
        messages = list()
        for identifier in feed_identifiers:
            messages.append(OPDSMessage(
                identifier.urn, HTTP_ACCEPTED, "Metadata needed."
            ))

        title = "%s Metadata Requests for %s" % (collection.protocol, client.url)
        metadata_request_url = self.collection_feed_url(
            'metadata_needed_for', collection
        )

        request_feed = AcquisitionFeed(
            self._db, title, metadata_request_url, [], VerboseAnnotator,
            precomposed_entries=messages
        )

        self.add_pagination_links_to_feed(
            pagination, unresolved_identifiers, request_feed,
            'metadata_needed_for', collection
        )

        return feed_response(request_feed)

    def remove_items(self, collection_details):
        """Removes identifiers from a Collection's catalog"""
        client = authenticated_client_from_request(self._db)
        if isinstance(client, ProblemDetail):
            return client

        collection = collection_from_details(
            self._db, client, collection_details
        )

        urns = request.args.getlist('urn')
        messages = []
        identifiers_by_urn, failures = Identifier.parse_urns(self._db, urns)

        for urn in failures:
            message = OPDSMessage(
                urn, INVALID_URN.status_code, INVALID_URN.detail
            )
            messages.append(message)

        # Find the IDs of the subset of provided identifiers that are
        # in the catalog, so we know which ones to delete and give a
        # 200 message. Also get a SQLAlchemy clause that selects only
        # those IDs.
        matching_ids, identifier_match_clause = self._in_catalog_subset(
            collection, identifiers_by_urn
        )

        # Use that clause to delete all of the relevant catalog
        # entries.
        delete_stmt = collections_identifiers.delete().where(
            identifier_match_clause
        )
        self._db.execute(delete_stmt)

        # IDs that matched get a 200 message; all others get a 404
        # message.
        for urn, identifier in identifiers_by_urn.items():
            if identifier.id in matching_ids:
                status = HTTP_OK
                description = "Successfully removed"
            else:
                status = HTTP_NOT_FOUND
                description = "Not in catalog"
            message = OPDSMessage(urn, status, description)
            messages.append(message)

        title = "%s Catalog Item Removal for %s" % (collection.protocol, client.url)
        url = self.collection_feed_url("remove", collection, urn=urns)
        removal_feed = AcquisitionFeed(
            self._db, title, url, [], VerboseAnnotator,
            precomposed_entries=messages
        )

        return feed_response(removal_feed)

    def _in_catalog_subset(self, collection, identifiers_by_urn):
        """Helper method to find a subset of identifiers that
        are already in a catalog.

        :param collection: The collection whose catalog we're checking.

        :param identifiers_by_urn: A dictionary mapping URNs to identifiers,
        like the one returned by Identifier.parse_urns.

        :return: a 2-tuple (in_catalog_ids,
        match_clause). `in_catalog_ids` is a set of Identifier IDs
        representing the subset of identifiers currently in the
        catalog. `match_clause` is the SQLAlchemy clause that was used
        to look up the matching subset.
        """
        # Extract the identifier IDs from the dictionary.
        identifier_ids = [x.id for x in identifiers_by_urn.values()]

        # Find the IDs for the subset of identifiers that are in the
        # catalog.
        table = collections_identifiers.c
        identifier_match_clause = and_(
            table.identifier_id.in_(identifier_ids),
            table.collection_id == collection.id
        )
        qu = self._db.query(collections_identifiers).filter(
            identifier_match_clause
        )
        matching_ids = [x[1] for x in qu]
        return matching_ids, identifier_match_clause

    def register(self, do_get=HTTP.get_with_timeout):

        # 'url' points to a document containing a public key that
        # should be used to sign the secret.
        public_key_url = request.form.get('url')
        if not public_key_url:
            return NO_AUTH_URL

        log = logging.getLogger(
            "Public key integration document (%s)" % public_key_url
        )

        # 'jwt' is a JWT that proves the client making this request
        # controls the private key.
        #
        # For backwards compatibility purposes, it's okay not to
        # provide a JWT, but it may lead to situations where
        # the client doesn't know their shared secret and can't reset
        # it.
        jwt_token = request.form.get('jwt')

        try:
            response = do_get(
                public_key_url, allowed_response_codes=['2xx', '3xx']
            )
        except Exception as e:
            log.error("Error retrieving URL", exc_info=e)
            return REMOTE_INTEGRATION_ERROR

        content_type = None
        if response.headers:
            content_type = response.headers.get('Content-Type')

        if not (response.content and content_type == OPDS_2_MEDIA_TYPE):
            # There's no JSON to speak of.
            log.error("Could not find OPDS 2 document: %s/%s",
                      response.content, content_type)
            return INVALID_INTEGRATION_DOCUMENT

        public_key_response = response.json()

        url = public_key_response.get('id')
        if not url:
            message = _("The public key integration document is missing an id.")
            log.error(unicode(message))
            return INVALID_INTEGRATION_DOCUMENT.detailed(message)

        # Remove any library-specific URL elements.
        def base_url(full_url):
            scheme, netloc, path, parameters, query, fragment = urlparse.urlparse(full_url)
            return '%s://%s' % (scheme, netloc)

        client_url = base_url(url)
        base_public_key_url = base_url(public_key_url) 
        if not client_url == base_public_key_url:
            log.error(
                "ID of OPDS 2 document (%s) doesn't match submitted URL (%s)", 
                client_url, base_public_key_url
            )
            return INVALID_INTEGRATION_DOCUMENT.detailed(
                _("The public key integration document id doesn't match submitted url")
            )

        public_key = public_key_response.get('public_key')
        if not (public_key and public_key.get('type') == 'RSA' and public_key.get('value')):
            message = _("The public key integration document is missing an RSA public_key.")
            log.error(unicode(message))
            return INVALID_INTEGRATION_DOCUMENT.detailed(message)
        public_key_text = public_key.get('value')
        public_key = RSA.importKey(public_key_text)
        encryptor = PKCS1_OAEP.new(public_key)

        submitted_secret = None
        auth_header = request.headers.get('Authorization')
        if auth_header and isinstance(auth_header, basestring) and 'bearer' in auth_header.lower():
            token = auth_header.split(' ')[1]
            submitted_secret = base64.b64decode(token)

        if jwt_token:
            # In the new system, the 'token' must be a JWT whose
            # signature can be verified with the public key.
            try:
                parsed = jwt.decode(
                    jwt_token, public_key_text, algorithm='RS256'
                )
            except Exception, e:
                return INVALID_CREDENTIALS.detailed(
                    _("Error decoding JWT: %s") % e.message
                )

            # The ability to create a valid JWT indicates control over
            # the server, so it's not necessary to know the current
            # secret to set a new secret.
            client, is_new = IntegrationClient.for_url(self._db, url)
            client.randomize_secret()
        else:
            # If no JWT is provided, then we use the old logic. The first
            # time registration happens, no special authentication
            # is required apart from the ability to decode the secret.
            #
            # On subsequent attempts, the old secret must be provided to
            # create a new secret.
            try:
                client, is_new = IntegrationClient.register(
                    self._db, url, submitted_secret=submitted_secret
                )
            except ValueError as e:
                log.error("Error in IntegrationClient.register", exc_info=e)
                return INVALID_CREDENTIALS.detailed(e.message)

        # Now that we have an IntegrationClient with a shared
        # secret, encrypt the shared secret with the provided public key
        # and send it back.
        encrypted_secret = encryptor.encrypt(str(client.shared_secret))
        shared_secret = base64.b64encode(encrypted_secret)
        auth_data = dict(
            id=url,
            metadata=dict(shared_secret=shared_secret)
        )
        content = json.dumps(auth_data)
        headers = { 'Content-Type' : OPDS_2_MEDIA_TYPE }

        status_code = 200
        if is_new:
            status_code = 201

        return make_response(content, status_code, headers)


class URNLookupController(CoreURNLookupController, ISBNEntryMixin):

    UNRESOLVABLE_IDENTIFIER = "I can't gather information about an identifier of this type."
    IDENTIFIER_REGISTERED = "You're the first one to ask about this identifier. I'll try to find out about it."
    WORKING_TO_RESOLVE_IDENTIFIER = "I'm working to locate a source for this identifier."
    SUCCESS_DID_NOT_RESULT_IN_PRESENTATION_READY_WORK = "Something's wrong. I have a record of covering this identifier but there's no presentation-ready work to show you."

    log = logging.getLogger("URN lookup controller")

    def __init__(self, _db):
        self.default_collection_id = None
        super(URNLookupController, self).__init__(_db)

    @property
    def default_collection(self):
        if getattr(self, '_default_collection_id', None) is None:
            default_collection, ignore = IdentifierResolutionCoverageProvider.unaffiliated_collection(self._db)
            self._default_collection_id = default_collection.id
        return get_one(self._db, Collection, id=self._default_collection_id)

    def presentation_ready_work_for(self, identifier):
        """Either return a presentation-ready work associated with the 
        given `identifier`, or return None.
        """
        pools = identifier.licensed_through
        if not pools:
            return None
        # All LicensePools for a given Identifier have the same Work.
        work = pools[0].work
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

    def process_urns(self, urns, collection_details=None, **kwargs):
        """Processes URNs submitted via lookup request

        An authenticated request can process up to 30 URNs at once,
        but must specify a collection under which to catalog the
        URNs. This is used when initially recording the fact that
        certain URNs are in a collection, to get a baseline set of
        metadata. Updates on the books should be obtained through the
        CatalogController.

        An unauthenticated request is used for testing. Such a request
        does not have to specify a collection (the "Unaffiliated"
        collection is used), but can only process one URN at a time.

        :return: None or ProblemDetail

        """
        client = authenticated_client_from_request(self._db, required=False)
        if isinstance(client, ProblemDetail):
            return client

        collection = collection_from_details(
            self._db, client, collection_details
        )

        if client:
            # Authenticated access.
            if not collection:
                return INVALID_INPUT.detailed(_("No collection provided."))
            limit = 30
        else:
            # Anonymous access.
            collection = self.default_collection
            limit = 1

        if len(urns) > limit:
            return INVALID_INPUT.detailed(
                _("The maximum number of URNs you can provide at once is %d. (You sent %d)") % (limit, len(urns))
            )
        identifiers_by_urn, failures = Identifier.parse_urns(self._db, urns)
        self.add_urn_failure_messages(failures)

        collection.catalog_identifiers(identifiers_by_urn.values())
        self.bulk_load_coverage_records(identifiers_by_urn.values())

        for urn, identifier in identifiers_by_urn.items():
            self.process_identifier(identifier, urn, **kwargs)

    def process_identifier(self, identifier, urn, **kwargs):
        """Turn an Identifier into a Work suitable for use in an OPDS feed.
        """
        if not self.can_resolve_identifier(identifier):
            return self.add_message(urn, HTTP_NOT_FOUND, self.UNRESOLVABLE_IDENTIFIER)

        if (identifier.type == Identifier.ISBN and not identifier.work):
            # There's not always enough information about an ISBN to
            # create a full Work. If not, we scrape together the cover
            # and description information and force the entry.
            entry = self.make_opds_entry_from_metadata_lookups(identifier)
            if not entry:
                return self.register_identifier_as_unresolved(
                    identifier.urn, identifier
                )
            return self.add_entry(entry)

        # All other identifiers need to be associated with a
        # presentation-ready Work for the lookup to succeed. If there
        # isn't one, we need to register it as unresolved.
        work = self.presentation_ready_work_for(identifier)
        if work:
            # The work has been done.
            return self.add_work(identifier, work)

        # Work remains to be done.
        return self.register_identifier_as_unresolved(urn, identifier)

    def bulk_load_coverage_records(self, identifiers):
        """Loads CoverageRecords for a list of identifiers into the database
        session in a single query before individual identifier processing
        begins.
        """
        identifier_ids = [i.id for i in identifiers]
        self._db.query(Identifier).filter(Identifier.id.in_(identifier_ids))\
            .options(joinedload(Identifier.coverage_records)).all()

    def register_identifier_as_unresolved(self, urn, identifier):
        # This identifier could have a presentation-ready Work
        # associated with it, but it doesn't. We need to make sure the
        # work gets done eventually by creating a CoverageRecord
        # representing the work that needs to be done.
        source = DataSource.lookup(self._db, DataSource.INTERNAL_PROCESSING)

        registration_records = filter(
            lambda c: c.operation==IdentifierResolutionRegistrar.OPERATION,
            identifier.coverage_records
        )
        if not (identifier.coverage_records or registration_records):
            # This identifier hasn't been registered for coverage yet.
            # Tell the client to come back later.
            return self.add_message(urn, HTTP_CREATED, self.IDENTIFIER_REGISTERED)

        resolution_records = filter(
            lambda c: c.operation==IdentifierResolutionCoverageProvider.OPERATION,
            identifier.coverage_records
        )
        resolution_record = None
        if resolution_records:
            resolution_record = resolution_records[0]

        # There is a pending attempt to resolve this identifier.
        # Tell the client we're working on it, or if the
        # pending attempt resulted in an exception,
        # tell the client about the exception.
        status = HTTP_ACCEPTED
        if (not resolution_record or resolution_record.status in
            (CoverageRecord.REGISTERED, CoverageRecord.TRANSIENT_FAILURE)
        ):
            return self.add_message(
                urn, status, self.WORKING_TO_RESOLVE_IDENTIFIER
            )

        # There is a pending attempt to resolve this identifier. Tell the
        # client we're working on it, or if the pending attempt resulted
        # in an exception, tell the client about the exception.
        message = resolution_record.exception
        if resolution_record.status == CoverageRecord.PERSISTENT_FAILURE:
            # Apparently we just can't provide coverage of this
            # identifier.
            status = HTTP_INTERNAL_SERVER_ERROR
        elif resolution_record.status == CoverageRecord.SUCCESS:
            # This shouldn't happen, since success in providing
            # this sort of coverage means creating a presentation
            # ready work. Something weird is going on.
            status = HTTP_INTERNAL_SERVER_ERROR
            message = self.SUCCESS_DID_NOT_RESULT_IN_PRESENTATION_READY_WORK
        return self.add_message(urn, status, message)

    def post_lookup_hook(self):
        """Run after looking up a number of Identifiers.

        We commit the database session because new Identifier and/or
        CoverageRecord objects may have been created during the
        lookup process.
        """
        self._db.commit()
