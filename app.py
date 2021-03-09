from nose.tools import set_trace
import os
import logging
import sys
from urllib.parse import urlparse

from flask import Flask
from flask_babel import Babel
from flask_sqlalchemy_session import flask_scoped_session

from core.app_server import (
    ErrorHandler,
)
from core.config import Configuration
from core.log import LogConfiguration
from core.model import SessionManager

app = Flask(__name__)
app._db = None
app.debug = None
babel = Babel(app)

from controller import MetadataWrangler

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

@app.before_first_request
def initialize_metadata_wrangler():
    if getattr(app, 'wrangler', None) is None:
        try:
            app.wrangler = MetadataWrangler(app._db)
        except Exception as e:
            logging.error(
                "Error instantiating metadata wrangler!", exc_info=e
            )
            raise e
        # Make sure that any changes to the database (as might happen
        # on initial setup) are committed before continuing.
        app.wrangler._db.commit()

@app.teardown_request
def shutdown_session(exception):
    if (hasattr(app, '_db')
        and app._db):
        if exception:
            app._db.rollback()
        else:
            app._db.commit()

from routes import *

def run(self, url=None, debug=False):
    base_url = url or 'http://localhost:5500/'
    scheme, netloc, path, parameters, query, fragment = urlparse(base_url)
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
