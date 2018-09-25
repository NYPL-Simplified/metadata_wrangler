from nose.tools import set_trace
import os
import logging
import sys
import urlparse

from functools import wraps
from flask import Flask
from flask_babel import Babel
from flask_sqlalchemy_session import flask_scoped_session

from core.app_server import (
    ErrorHandler,
    HeartbeatController,
    returns_problem_detail,
)
from core.config import Configuration
from core.log import LogConfiguration
from core.model import SessionManager
from core.opds import VerboseAnnotator
from core.util.problem_detail import ProblemDetail

from controller import (
    authenticated_client_from_request,
    CatalogController,
    CanonicalizationController,
    IndexController,
    URNLookupController
)


app = Flask(__name__)
app._db = None
app.debug = None
babel = Babel(app)

@app.before_first_request
def initialize_database(autoinitialize=True):
    db_url = Configuration.database_url()
    if autoinitialize:
        SessionManager.initialize(db_url)
    session_factory = SessionManager.sessionmaker(db_url)
    _db = flask_scoped_session(session_factory, app)
    app._db = _db

    Configuration.load(_db)
    testing = 'TESTING' in os.environ
    log_level = LogConfiguration.initialize(_db, testing=testing)
    if app.debug is None:
        debug = log_level == 'DEBUG'
        app.debug = debug
    else:
        debug = app.debug
    app.config['DEBUG'] = debug
    _db.commit()
    app.log = logging.getLogger("Metadata web app")
    app.log.info("Application debug mode: %r", app.debug)
    for logger in logging.getLogger().handlers:
        app.log.info("Logs are going to %r", logger)

    # Register an error handler that logs exceptions through the
    # normal logging process and tries to turn them into Problem
    # Detail Documents.
    h = ErrorHandler(app, app.config['DEBUG'])
    @app.errorhandler(Exception)
    def exception_handler(exception):
        return h.handle(exception)

def accepts_auth(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        client = authenticated_client_from_request(app._db, required=False)
        if isinstance(client, ProblemDetail):
            return client.response
        return f(*args, **kwargs)
    return decorated

def requires_auth(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        client = authenticated_client_from_request(app._db)
        if isinstance(client, ProblemDetail):
            return client.response
        return f(*args, **kwargs)
    return decorated

@app.teardown_request
def shutdown_session(exception):
    if (hasattr(app, '_db')
        and app._db):
        if exception:
            app._db.rollback()
        else:
            app._db.commit()

@app.route('/')
def index():
    return IndexController(app._db).opds_catalog()

@app.route('/heartbeat')
def heartbeat():
    return HeartbeatController().heartbeat()

@app.route('/canonical-author-name')
@returns_problem_detail
def canonical_author_name():
    return CanonicalizationController(app._db).canonicalize_author_name()

@app.route('/lookup')
@app.route('/<collection_metadata_identifier>/lookup')
@accepts_auth
@returns_problem_detail
def lookup(collection_metadata_identifier=None):
    return URNLookupController(app._db).work_lookup(
        VerboseAnnotator, require_active_licensepool=False,
        collection_details=collection_metadata_identifier
    )

@app.route('/<collection_metadata_identifier>/add', methods=['POST'])
@requires_auth
@returns_problem_detail
def add(collection_metadata_identifier):
    return CatalogController(app._db).add_items(
        collection_details=collection_metadata_identifier
    )

@app.route('/<collection_metadata_identifier>/add_with_metadata', methods=['POST'])
@requires_auth
@returns_problem_detail
def add_with_metadata(collection_metadata_identifier):
    return CatalogController(app._db).add_with_metadata(
        collection_details=collection_metadata_identifier
    )

@app.route('/<collection_metadata_identifier>/metadata_needed', methods=['GET'])
@requires_auth
@returns_problem_detail
def metadata_needed_for(collection_metadata_identifier):
    return CatalogController(app._db).metadata_needed_for(
        collection_details=collection_metadata_identifier
    )

@app.route('/<collection_metadata_identifier>/updates')
@requires_auth
@returns_problem_detail
def updates(collection_metadata_identifier):
    return CatalogController(app._db).updates_feed(
        collection_details=collection_metadata_identifier
    )

@app.route('/<collection_metadata_identifier>/remove', methods=['POST'])
@requires_auth
@returns_problem_detail
def remove(collection_metadata_identifier):
    return CatalogController(app._db).remove_items(
        collection_details=collection_metadata_identifier
    )

@app.route("/register", methods=["POST"])
@returns_problem_detail
def register():
    return CatalogController(app._db).register()

def run(self, url=None, debug=False):
    base_url = url or u'http://localhost:5500/'
    scheme, netloc, path, parameters, query, fragment = urlparse.urlparse(base_url)
    if ':' in netloc:
        host, port = netloc.split(':')
        port = int(port)
    else:
        host = netloc
        port = 80

    app.debug = debug
    # Workaround for a "Resource temporarily unavailable" error when
    # running in debug mode with the global socket timeout set by isbnlib
    if app.debug:
        import socket
        socket.setdefaulttimeout(None)

    logging.info("Starting app on %s:%s", host, port)
    app.run(debug=debug, host=host, port=port)

if __name__ == '__main__':
    url = None
    if len(sys.argv) > 1:
        url = sys.argv[1]
    run(url, debug=True)
