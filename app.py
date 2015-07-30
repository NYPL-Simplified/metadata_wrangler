from nose.tools import set_trace
import os
import logging
import flask
import urlparse

from flask import Flask, make_response
from core.util.flask_util import problem
from core.opds import VerboseAnnotator
from core.app_server import (
    HeartbeatController,
    URNLookupController,
)
from core.model import (
    production_session,
    Identifier,
)
from canonicalize import AuthorNameCanonicalizer

app = Flask(__name__)
app.config['DEBUG'] = True
app.debug = True


class Conf:
    db = None
    log = None

    @classmethod
    def initialize(cls, _db):
        cls.db = _db
        cls.log = logging.getLogger("Metadata web app")

if os.environ.get('TESTING') == "True":
    Conf.testing = True
else:
    Conf.testing = False
    _db = production_session()
    Conf.initialize(_db)

@app.route('/heartbeat')
def hearbeat():
    return HeartbeatController().heartbeat()

@app.route('/lookup')
def lookup():
    return URNLookupController(Conf.db, True).work_lookup(VerboseAnnotator)

@app.route('/canonical-author-name')
def canonical_author_name():
    urn = flask.request.args.get('urn')
    display_name = flask.request.args.get('display_name')
    if urn:
        identifier = URNLookupController.parse_urn(Conf.db, urn, False)
        if not isinstance(identifier, Identifier):
            # Error.
            status, title = identifier
            type = URNLookupController.COULD_NOT_PARSE_URN_TYPE
            return problem(type, title, status)
    else:
        identifier = None

    canonicalizer = AuthorNameCanonicalizer(Conf.db)
    author_name = canonicalizer.canonicalize(identifier, display_name)
    Conf.log.info("Incoming display name/identifier: %r/%s. Canonicalizer said: %s",
                  display_name, identifier, author_name)
    if not author_name:
        if display_name:
            author_name = canonicalizer.default_name(display_name)
            Conf.log.info("Defaulting to %s for %r", author_name, identifier)
    Conf.db.commit()
    if author_name:
        return make_response(author_name, 200, {"Content-Type": "text/plain"})
    else:
        return make_response("", 404)


if __name__ == '__main__':

    debug = True
    url = os.environ['METADATA_WEB_APP_URL']
    scheme, netloc, path, parameters, query, fragment = urlparse.urlparse(url)
    if ':' in netloc:
        host, port = netloc.split(':')
        port = int(port)
    else:
        host = netloc
        port = 80
    Conf.log.info("Starting app on %s:%s", host, port)
    app.run(debug=debug, host=host, port=port)
