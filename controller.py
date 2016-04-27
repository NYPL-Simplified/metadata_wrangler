from nose.tools import set_trace
from datetime import datetime
from flask import request, make_response

from core.app_server import (
    cdn_url_for,
    feed_response,
    load_pagination_from_request,
)
from core.model import (
    Collection,
    Identifier,
)
from core.opds import (
    AcquisitionFeed,
    VerboseAnnotator,
)
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
            update_feed.add_link(
                rel="next", href=update_url(page=pagination.next_page)
            )
        if pagination.offset > 0:
            update_feed.add_link(
                rel="first", href=update_url(page=pagination.first_page)
            )
        previous_page = pagination.previous_page
        if previous_page:
            update_feed.add_link(
                rel="previous", href=update_url(page=previous_page)
            )

        return feed_response(update_feed)

    def remove_items(self):
        collection = self.authenticated_collection_from_request()
        if isinstance(collection, ProblemDetail):
            return collection

        urns = request.args.getlist('urn')
        invalid_urns = []
        for urn in urns:
            identifier = None
            try:
                identifier, ignore = Identifier.parse_urn(self._db, urn)
            except Exception as e:
                invalid_urns.append(urn)
            if identifier and identifier in collection.catalog:
                collection.catalog.remove(identifier)
        self._db.commit()
        if invalid_urns:
            debug_message = "INVALID URN: " + ", ".join(invalid_urns)
            return INVALID_URN.with_debug(debug_message)
        return make_response("", 204, {})
