import flask
from nose.tools import set_trace
from datetime import datetime

from core.app_server import (
    cdn_url_for,
    feed_response,
)
from core.model import Collection
from core.opds import (
    AcquisitionFeed,
    VerboseAnnotator,
)
from core.util.problem_detail import ProblemDetail
from core.problem_details import INVALID_CREDENTIALS


class CollectionController(object):
    """A controller to manage collections and their assets"""

    def __init__(self, _db):
        self._db = _db

    def authenticated_collection_from_request(self, required=True):
        header = flask.request.authorization
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

        update_url = cdn_url_for('updates', _external=True)
        # Record time of update check before initiating database query.
        updated_at = datetime.utcnow()
        updated_works = collection.works_updated(self._db)
        collection.last_checked = updated_at
        self._db.commit()

        feed_title = "%s Updates" % collection.name
        update_feed = AcquisitionFeed(
            self._db, feed_title, update_url, updated_works,
            VerboseAnnotator
        )
        return feed_response(update_feed)
