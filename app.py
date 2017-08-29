from nose.tools import set_trace
import os
import logging
import flask
import urlparse

from functools import wraps
from flask import Flask
from flask.ext.babel import Babel
from core.util.problem_detail import ProblemDetail
from core.opds import VerboseAnnotator
from core.app_server import (
    HeartbeatController,
    returns_problem_detail,
)
from core.model import (
    ConfigurationSetting,
    production_session,
)
from core.config import Configuration

from controller import (
    authenticated_client_from_request,
    CatalogController,
    CanonicalizationController,
    URNLookupController
)


app = Flask(__name__)
app.config['DEBUG'] = True
app.debug = True
babel = Babel(app)

class Conf:
    db = None
    log = None

    @classmethod
    def initialize(cls, _db):
        cls.db = _db
        Configuration.load(cls.db)
        cls.log = logging.getLogger("Metadata web app")

if os.environ.get('TESTING') == "true":
    Conf.testing = True
else:
    Conf.testing = False
    _db = production_session()
    Conf.initialize(_db)


def accepts_auth(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        client = authenticated_client_from_request(Conf.db, required=False)
        if isinstance(client, ProblemDetail):
            return client.response
        return f(*args, **kwargs)
    return decorated

def requires_auth(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        client = authenticated_client_from_request(Conf.db)
        if isinstance(client, ProblemDetail):
            return client.response
        return f(*args, **kwargs)
    return decorated

@app.teardown_request
def shutdown_session(exception):
    if (hasattr(Conf, 'db')
        and Conf.db):
        if exception:
            Conf.db.rollback()
        else:
            Conf.db.commit()

@app.route('/heartbeat')
def heartbeat():
    return HeartbeatController().heartbeat()

@app.route('/canonical-author-name')
@returns_problem_detail
def canonical_author_name():
    return CanonicalizationController(Conf.db).canonicalize_author_name()

@app.route('/lookup')
@app.route('/<collection_metadata_identifier>/lookup')
@accepts_auth
@returns_problem_detail
def lookup(collection_metadata_identifier=None):
    return URNLookupController(Conf.db).work_lookup(
        VerboseAnnotator, require_active_licensepool=False,
        collection_details=collection_metadata_identifier
    )

@app.route('/<collection_metadata_identifier>/add', methods=['POST'])
@requires_auth
@returns_problem_detail
def add(collection_metadata_identifier):
    return CatalogController(Conf.db).add_items(
        collection_details=collection_metadata_identifier
    )

@app.route('/<collection_metadata_identifier>/updates')
@requires_auth
@returns_problem_detail
def updates(collection_metadata_identifier):
    return CatalogController(Conf.db).updates_feed(
        collection_details=collection_metadata_identifier
    )

@app.route('/<collection_metadata_identifier>/remove', methods=['POST'])
@requires_auth
@returns_problem_detail
def remove(collection_metadata_identifier):
    return CatalogController(Conf.db).remove_items(
        collection_details=collection_metadata_identifier
    )

@app.route('/client/update_url', methods=['POST'])
@requires_auth
@returns_problem_detail
def update_url():
    return CatalogController(Conf.db).update_client_url()

@app.route("/register", methods=["POST"])
@returns_problem_detail
def register():
    return CatalogController(Conf.db).register()


if __name__ == '__main__':

    debug = True
    url = ConfigurationSetting.sitewide(Conf.db, Configuration.BASE_URL_KEY).value
    scheme, netloc, path, parameters, query, fragment = urlparse.urlparse(url)
    if ':' in netloc:
        host, port = netloc.split(':')
        port = int(port)
    else:
        host = netloc
        port = 80

    # Workaround for a "Resource temporarily unavailable" error when
    # running in debug mode with the global socket timeout set by isbnlib
    if debug:
        import socket
        socket.setdefaulttimeout(None)

    Conf.log.info("Starting app on %s:%s", host, port)
    app.run(debug=debug, host=host, port=port)
