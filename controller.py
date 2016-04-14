import flask
from core.model import (
    get_one,
    Library,
)
from core.util.problem_detail import ProblemDetail as pd
from core.problem_details import INVALID_CREDENTIALS

class LibraryController(object):
    """Handles routes and resources related to authenticated libraries."""

    def __init__(self, _db):
        self._db = _db

    def authenticated_library(self):
        header = flask.request.authorization
        if header:
            client_id = header.username
            client_secret = header.password
            return get_one(
                self._db, Library,
                client_id=client_id,
                client_secret=client_secret
            )

    @classmethod
    def invalid_credentials(cls):
        type = INVALID_CREDENTIALS.uri
        title = INVALID_CREDENTIALS.title
        status = INVALID_CREDENTIALS.status_code
        return problem(type, status, title)
