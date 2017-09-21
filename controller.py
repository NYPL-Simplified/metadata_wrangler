from nose.tools import set_trace
from datetime import datetime
from flask import request, make_response
from lxml import etree
from Crypto.PublicKey import RSA
from Crypto.Cipher import PKCS1_OAEP
import base64
import feedparser
import json
import logging
import urllib
import urlparse

from core.app_server import (
    cdn_url_for,
    feed_response,
    load_pagination_from_request,
    URNLookupController as CoreURNLookupController,
)
from core.config import Configuration
from core.model import (
    Collection,
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
from core.util.authentication_for_opds import AuthenticationForOPDSDocument
from core.util.http import HTTP
from core.util.opds_writer import OPDSMessage
from core.util.problem_detail import ProblemDetail

from coverage import (
    IdentifierResolutionCoverageProvider,
    IdentifierResolutionRegistrar,
)
from canonicalize import AuthorNameCanonicalizer
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
                "href": "/lookup{?urn*}",
                "type": "application/atom+xml;profile=opds-catalog",
                "title": "Look up metadata about one or more specific items",
                "templated": "true"
            },
            {
                "rel": "http://opds-spec.org/sort/new",
                "href": "/{collection_metadata_identifier}/updates",
                "type": "application/atom+xml;profile=opds-catalog",
                "title": "Recent changes to one of your tracked collections.",
                "templated": "true"
            },
            {
                "rel": "http://librarysimplified.org/rel/metadata/collection-add",
                "href": "/{collection_metadata_identifier}/add{?urn*}",
                "title": "Add items to one of your tracked collections.",
                "templated": "true"
            },
            {
                "rel": "http://librarysimplified.org/rel/metadata/collection-remove",
                "href": "/{collection_metadata_identifier}/remove{?urn*}",
                "title": "Remove items from one of your tracked collections.",
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


class CatalogController(object):
    """A controller to manage a Collection's catalog"""

    def __init__(self, _db):
        self._db = _db

    def updates_feed(self, collection_details):
        client = authenticated_client_from_request(self._db)
        if isinstance(client, ProblemDetail):
            return client

        collection = collection_from_details(
            self._db, client, collection_details
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

        entries = []
        for work in works[:]:
            entry = work.verbose_opds_entry or work.simple_opds_entry
            entry = etree.fromstring(entry)
            if entry:
                entries.append(entry)
                works.remove(work)

        works = [(work.identifier, work) for work in works]

        update_feed = LookupAcquisitionFeed(
            self._db, title, update_url(), works, VerboseAnnotator,
            precomposed_entries=entries
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
        for urn in urns:
            message = None
            identifier = None
            try:
                identifier, ignore = Identifier.parse_urn(
                    self._db, urn
                )
            except Exception as e:
                identifier = None

            if not identifier:
                message = OPDSMessage(
                    urn, INVALID_URN.status_code, INVALID_URN.detail
                )
            else:
                status = HTTP_OK
                description = "Already in catalog"

                if identifier not in collection.catalog:
                    collection.catalog_identifier(self._db, identifier)
                    status = HTTP_CREATED
                    description = "Successfully added"

                message = OPDSMessage(urn, status, description)

            messages.append(message)

        title = "%s Catalog Item Additions for %s" % (collection.protocol, client.url)
        url = cdn_url_for(
            "add", collection_metadata_identifier=collection.name, urn=urns
        )
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

        messages = []

        feed = feedparser.parse(request.data)
        entries = feed.get("entries", [])

        if not client.data_source:
            client.data_source = DataSource.lookup(self._db, client.url, autocreate=True)
        data_source = client.data_source

        for entry in entries:
            urn = entry.get('id')
            try:
                identifier, ignore = Identifier.parse_urn(
                    self._db, urn
                )
            except Exception as e:
                identifier = None

            if not identifier:
                message = OPDSMessage(
                    urn, INVALID_URN.status_code, INVALID_URN.detail
                )
            else:
                status = HTTP_OK
                description = "Already in catalog"

                if identifier not in collection.catalog:
                    collection.catalog_identifier(self._db, identifier)
                    status = HTTP_CREATED
                    description = "Successfully added"

                message = OPDSMessage(urn, status, description)

                # Make sure there's a LicensePool for this Identifier in this
                # Collection.
                license_pools = [p for p in identifier.licensed_through
                                 if collection==p.collection]
            
                if license_pools:
                    # A given Collection may have at most one LicensePool for
                    # a given identifier.
                    pool = license_pools[0]
                else:
                    # This Collection has no LicensePool for the given Identifier.
                    # Create one.
                    pool, ignore = LicensePool.for_foreign_id(
                        self._db, data_source, identifier.type, 
                        identifier.identifier, collection=collection
                    )


                # Create an edition to hold the title and author. LicensePool.calculate_work
                # refuses to create a Work when there's no title, and if we have a title, author
                # and language we can attempt to look up the edition in OCLC.
                images = [l for l in entry.get("links", []) if l.get("rel") == Hyperlink.IMAGE or l.get("rel") == Hyperlink.THUMBNAIL_IMAGE]
                links = [LinkData(image.get("rel"), image.get("href")) for image in images]
                author = ContributorData(sort_name=(entry.get("author") or Edition.UNKNOWN_AUTHOR),
                                         roles=[Contributor.PRIMARY_AUTHOR_ROLE])

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
                    title=entry.get("title") or "Unknown",
                    language=entry.get("dcterms_language"),
                    contributors=[author],
                    links=links,
                )

                edition, ignore = metadata.edition(self._db)
                metadata.apply(edition, collection, replace=replace)

                # Create a transient failure CoverageRecord for this identifier
                # so it will be processed by the IdentifierResolutionCoverageProvider.
                internal_processing = DataSource.lookup(self._db, DataSource.INTERNAL_PROCESSING)
                CoverageRecord.add_for(edition, internal_processing,
                                       operation=CoverageRecord.RESOLVE_IDENTIFIER_OPERATION,
                                       status=CoverageRecord.TRANSIENT_FAILURE,
                                       collection=collection)

            messages.append(message)

        title = "%s Catalog Item Additions for %s" % (collection.protocol, client.url)
        url = cdn_url_for(
            "add_with_metadata", collection_metadata_identifier=collection.name
        )
        addition_feed = AcquisitionFeed(
            self._db, title, url, [], VerboseAnnotator,
            precomposed_entries=messages
        )

        return feed_response(addition_feed)

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

            messages.append(message)

        title = "%s Catalog Item Removal for %s" % (collection.protocol, client.url)
        url = cdn_url_for("remove", collection_metadata_identifier=collection.name, urn=urns)
        removal_feed = AcquisitionFeed(
            self._db, title, url, [], VerboseAnnotator,
            precomposed_entries=messages
        )

        return feed_response(removal_feed)

    def register(self, do_get=HTTP.get_with_timeout):
        public_key_url = request.form.get('url')
        if not public_key_url:
            return NO_AUTH_URL

        try:
            response = do_get(
                public_key_url, allowed_response_codes=['2xx', '3xx']
            )
        except Exception as e:
            return REMOTE_INTEGRATION_ERROR

        content_type = None
        if response.headers:
            content_type = response.headers.get('Content-Type')

        if not (response.content and content_type == OPDS_2_MEDIA_TYPE):
            # There's no JSON to speak of.
            return INVALID_INTEGRATION_DOCUMENT

        public_key_response = response.json()

        url = public_key_response.get('id')
        if not url:
            return INVALID_INTEGRATION_DOCUMENT.detailed(
                "The public key integration document is missing an id."
            )

        # Remove any library-specific URL elements.
        def base_url(full_url):
            scheme, netloc, path, parameters, query, fragment = urlparse.urlparse(full_url)
            return '%s://%s' % (scheme, netloc)

        client_url = base_url(url)
        if not client_url == base_url(public_key_url):
            return INVALID_INTEGRATION_DOCUMENT.detailed(
                "The public key integration document id doesn't match submitted url"
            )

        public_key = public_key_response.get('public_key')
        if not (public_key and public_key.get('type') == 'RSA' and public_key.get('value')):
            return INVALID_INTEGRATION_DOCUMENT.detailed(
                "The public key integration document is missing an RSA public_key."
            )
        public_key = RSA.importKey(public_key.get('value'))
        encryptor = PKCS1_OAEP.new(public_key)

        submitted_secret = None
        auth_header = request.headers.get('Authorization')
        if auth_header and isinstance(auth_header, basestring) and 'bearer' in auth_header.lower():
            token = auth_header.split(' ')[1]
            submitted_secret = base64.b64decode(token)

        try:
            client, is_new = IntegrationClient.register(
                self._db, url, submitted_secret=submitted_secret
            )
        except ValueError as e:
            return INVALID_CREDENTIALS.detailed(repr(e))

        # Encrypt shared secret.
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


class URNLookupController(CoreURNLookupController):

    UNRESOLVABLE_IDENTIFIER = "I can't gather information about an identifier of this type."
    IDENTIFIER_REGISTERED = "You're the first one to ask about this identifier. I'll try to find out about it."
    WORKING_TO_RESOLVE_IDENTIFIER = "I'm working to locate a source for this identifier."
    SUCCESS_DID_NOT_RESULT_IN_PRESENTATION_READY_WORK = "Something's wrong. I have a record of covering this identifier but there's no presentation-ready work to show you."

    log = logging.getLogger("URN lookup controller")

    def __init__(self, _db):
        self.registrar = IdentifierResolutionRegistrar(_db)
        super(URNLookupController, self).__init__(_db)

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
        if isinstance(client, ProblemDetail):
            return client

        collection = collection_from_details(
            self._db, client, collection_details
        )
        if collection:
            collection.catalog_identifier(self._db, identifier)

        if (identifier.type == Identifier.ISBN and not identifier.work):
            # There's not always enough information about an ISBN to
            # create a full Work. If not, we scrape together the cover
            # and description information and force the entry.
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

        record, is_new = self.registrar.register(identifier)
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
            if not message or message == self.registrar.NO_WORK_DONE_EXCEPTION:
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
