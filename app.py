from nose.tools import set_trace
import os
import flask
from flask import Flask, make_response
from core.util.flask_util import problem
from core.opds import VerboseAnnotator
from core.app_server import URNLookupController
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

    @classmethod
    def initialize(cls, _db):
        cls.db = _db

if os.environ.get('TESTING') == "True":
    Conf.testing = True
else:
    Conf.testing = False
    _db = production_session()
    Conf.initialize(_db)

@app.route('/lookup')
def lookup():
    return URNLookupController(Conf.db, True).work_lookup(VerboseAnnotator)

@app.route('/canonical-author-name')
def canonical_author_name():
    urn = flask.request.args.get('urn')
    display_name = flask.request.args.get('display_name')
    identifier = URNLookupController.parse_urn(Conf.db, urn, False)
    if not isinstance(identifier, Identifier):
        # Error.
        status, title = identifier
        type = URNLookupController.COULD_NOT_PARSE_URN_TYPE
        return problem(type, title, status)
        
    canonicalizer = AuthorNameCanonicalizer(Conf.db)
    print "Incoming display name: %s" % display_name
    print "Incoming identifier: %r" % identifier
    author_name = canonicalizer.canonicalize(identifier, display_name)
    print "Canonicalizer said: %s" % author_name
    if not author_name:
        if display_name:
            author_name = canonicalizer.default_name(display_name)
            print "Defaulting to: %s" % author_name
    if author_name:
        return make_response(author_name, 200, {"Content-Type": "text/plain"})
    else:
        return make_response("", 404)


if __name__ == '__main__':

    debug = True
    host = "0.0.0.0"
    port = int(os.environ['METADATA_WEB_APP_PORT'])
    app.run(debug=debug, host=host, port=port)
