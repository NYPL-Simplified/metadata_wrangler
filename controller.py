import flask
from nose.tools import set_trace
from datetime import datetime

from core.app_server import (
    cdn_url_for,
    feed_response,
)
from core.model import Collection
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
