from nose.tools import set_trace
from functools import wraps

from core.app_server import (
    HeartbeatController,
    returns_problem_detail,
)

from core.opds import VerboseAnnotator
from core.util.problem_detail import ProblemDetail

from controller import (
    authenticated_client_from_request,
    CatalogController,
    CanonicalizationController,
    IndexController,
    IntegrationClientController,
    URNLookupController
)

from app import app

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
    return IntegrationClientController(app._db).register()
