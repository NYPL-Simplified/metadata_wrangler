from nose.tools import set_trace
from datetime import datetime
from flask import request, make_response
import logging

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
    UnresolvedIdentifier,
)
from core.opds import (
    AcquisitionFeed,
    VerboseAnnotator,
)
from core.util.opds_writer import OPDSMessage
from core.util.problem_detail import ProblemDetail
from core.problem_details import (
    INVALID_CREDENTIALS,
    INVALID_URN,
)


class CollectionController(object):
    """A controller to manage collections and their assets"""

    def __init__(self, _db):
        self._db = _db

    def authenticated_collection_from_request(self, required=True):
        header = request.authorization
        if header:
            client_id, client_secret = header.username, header.password
            collection = Collection.authenticate(self._db, client_id, client_secret)
            if collection:
                return collection
        if not required and not header:
            # In the case that authentication is not required
            # (i.e. URN lookup) return None instead of an error.
            return None
        return INVALID_CREDENTIALS

    def updates_feed(self):
        collection = self.authenticated_collection_from_request()
        if isinstance(collection, ProblemDetail):
            return collection

        last_update_time = request.args.get('last_update_time', None)
        if last_update_time:
            last_update_time = datetime.strptime(last_update_time, "%Y-%m-%dT%H:%M:%SZ")
        updated_works = collection.works_updated_since(self._db, last_update_time)

        pagination = load_pagination_from_request()
        works = pagination.apply(updated_works).all()
        title = "%s Updates" % collection.name
        def update_url(time=last_update_time, page=None):
            kw = dict(_external=True)
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

    def remove_items(self):
        collection = self.authenticated_collection_from_request()
        if isinstance(collection, ProblemDetail):
            return collection

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
                        urn, 200, "Successfully removed"
                    )
                else:
                    message = OPDSMessage(
                        urn, 404, "Not in collection catalog"
                    )
            if message:
                messages.append(message)

        title = "%s Catalog Item Removal" % collection.name
        url = cdn_url_for("remove", urn=urns)
        removal_feed = AcquisitionFeed(
            self._db, title, url, [], VerboseAnnotator,
            precomposed_entries=messages
        )

        return feed_response(removal_feed)


class URNLookupController(CoreURNLookupController):

    UNRESOLVABLE_IDENTIFIER = "I can't gather information about an identifier of this type."
    IDENTIFIER_REGISTERED = "You're the first one to ask about this identifier. I'll try to find out about it."
    WORKING_TO_RESOLVE_IDENTIFIER = "I'm working to locate a source for this identifier."

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
  
    def process_urn(self, urn, collection=None, **kwargs):
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
            return self.add_message(urn, 404, self.UNRESOLVABLE_IDENTIFIER)

        # We are at least willing to try to resolve this Identifier.
        # If a Collection was provided, this also means we consider
        # this Identifier part of the given collection.
        if collection:
            collection.catalog_identifier(self._db, identifier)
        
        if identifier.type == Identifier.ISBN:
            # ISBNs are handled specially.
            return self.make_opds_entry_from_metadata_lookups(identifier)

        # All other identifiers need to be associated with a
        # presentation-ready Work for the lookup to succeed. If there
        # isn't one, we need to create an UnresolvedIdentifier object.
        work = self.presentation_ready_work_for(identifier)
        if work:
            # The work has been done.
            return self.add_work(identifier, work)

        # Work remains to be done.
        return self.register_identifier_as_unresolved(urn, identifier)

    def register_identifier_as_unresolved(self, urn, identifier):
        # This identifier could have a presentation-ready Work
        # associated with it, but it doesn't. Make sure an
        # UnresolvedIdentifier is registered for it so the work can
        # begin.
        unresolved_identifier, is_new = UnresolvedIdentifier.register(
            self._db, identifier
        )
        if is_new:
            # The identifier is newly registered. Tell the client
            # to come back later.
            return self.add_message(urn, 201, self.IDENTIFIER_REGISTERED)
        else:
            # There is a pending attempt to resolve this identifier.
            # Tell the client we're working on it, or if the
            # pending attempt resulted in an exception,
            # tell the client about the exception.
            message = (unresolved_identifier.exception 
                       or self.WORKING_TO_RESOLVE_IDENTIFIER)
            return self.add_message(
                urn, unresolved_identifier.status, message
            )

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
            unresolved_identifier, is_new = UnresolvedIdentifier.register(
                self._db, identifier)
            if is_new:
                # We just found out about this identifier, or rather,
                # we just found out that someone expects it to be associated
                # with a LicensePool.
                return self.add_message(
                    identifier.urn, 201, self.IDENTIFIER_REGISTERED
                )
            else:
                # There is a pending attempt to resolve this identifier.
                message = (unresolved_identifier.exception 
                           or self.WORKING_TO_RESOLVE_IDENTIFIER)
                return self.add_message(
                    identifier.urn, unresolved_identifier.status, message
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
                identifier.urn, 404, self.UNRECOGNIZED_IDENTIFIER
            )

        # We made it!
        return self.add_entry(entry)

    def post_lookup_hook(self):
        """Run after looking up a number of Identifiers.

        We commit the database session because new Identifier or
        UnresolvedIdentifier objects may have been created during the
        lookup process.
        """
        self._db.commit()
