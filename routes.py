from nose.tools import set_trace
from functools import wraps

from core.app_server import (
    HeartbeatController,
    returns_problem_detail,
)

from core.opds import VerboseAnnotator
from core.util.problem_detail import ProblemDetail

from app import app

def accepts_auth(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        client = app.wrangler.authenticated_client_from_request(
            required=False
        )
        if isinstance(client, ProblemDetail):
            return client.response
        return f(*args, **kwargs)
    return decorated

def requires_auth(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        client = app.wrangler.authenticated_client_from_request(required=True)
        if isinstance(client, ProblemDetail):
            return client.response
        return f(*args, **kwargs)
    return decorated

@app.route('/', strict_slashes=False)
def index():
    return app.wrangler.index.opds_catalog()

@app.route('/heartbeat')
def heartbeat():
    return app.wrangler.heartbeat.heartbeat()

@app.route('/canonical-author-name')
@returns_problem_detail
def canonical_author_name():
    return app.wrangler.canonicalization.canonicalize_author_name()

@app.route('/lookup')
@app.route('/<collection_metadata_identifier>/lookup')
@accepts_auth
@returns_problem_detail
def lookup(collection_metadata_identifier=None):
    return app.wrangler.urn_lookup.work_lookup(
        VerboseAnnotator, require_active_licensepool=False,
        metadata_identifier=collection_metadata_identifier
    )

@app.route('/<collection_metadata_identifier>/add', methods=['POST'])
@requires_auth
@returns_problem_detail
def add(collection_metadata_identifier):
    return app.wrangler.catalog.add_items(
        metadata_identifier=collection_metadata_identifier
    )

@app.route('/<collection_metadata_identifier>/add_with_metadata', methods=['POST'])
@requires_auth
@returns_problem_detail
def add_with_metadata(collection_metadata_identifier):
    return app.wrangler.catalog.add_with_metadata(
        metadata_identifier=collection_metadata_identifier
    )

@app.route('/<collection_metadata_identifier>/metadata_needed', methods=['GET'])
@requires_auth
@returns_problem_detail
def metadata_needed_for(collection_metadata_identifier):
    return app.wrangler.catalog.metadata_needed_for(
        metadata_identifier=collection_metadata_identifier
    )

@app.route('/<collection_metadata_identifier>/updates')
@requires_auth
@returns_problem_detail
def updates(collection_metadata_identifier):
    return app.wrangler.catalog.updates_feed(
        metadata_identifier=collection_metadata_identifier
    )

@app.route('/<collection_metadata_identifier>/remove', methods=['POST'])
@requires_auth
@returns_problem_detail
def remove(collection_metadata_identifier):
    return app.wrangler.catalog.remove_items(
        metadata_identifier=collection_metadata_identifier
    )

@app.route("/register", methods=["POST"])
@returns_problem_detail
def register():
    return app.wrangler.integration.register()
