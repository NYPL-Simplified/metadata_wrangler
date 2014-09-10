from functools import wraps
from nose.tools import set_trace
import os
import sys

from sqlalchemy.orm.exc import (
    NoResultFound
)

import flask
from flask import Flask, url_for, redirect, Response
from jinja2 import Environment, PackageLoader

from model import (
    get_one_or_create,
    DataSource,
    production_session,
    LaneList,
    Lane,
    LicensePool,
    Patron,
    WorkIdentifier,
    Work,
    WorkFeed,
    WorkRecord,
    )
from opensearch import OpenSearchDocument
from opds import (
    E,
    AcquisitionFeed,
    NavigationFeed,
    URLRewriter,
)
import urllib
from util import LanguageCodes
from util import problem_detail
from integration.millenium_patron import DummyMilleniumPatronAPI as authenticator
import classifier as genres
from classifier import (
    Classifier,
    GenreData,
)

auth = authenticator()

class Conf:
    db = None
    lanes = None

    @classmethod
    def initialize(cls, _db, lanes):
        cls.db = _db
        cls.lanes = lanes

if os.environ.get('TESTING') != "True":
    _db = production_session()
    lanes = LaneList.from_description(
        _db,
        [dict(name="Fiction",
              fiction=True,
              audience=genres.Classifier.AUDIENCE_ADULT,
              genres=None),
         genres.Biography_Memoir,
         genres.Crime_Thrillers_Mystery,
         dict(name="Nonfiction",
              fiction=False,
              audience=genres.Classifier.AUDIENCE_ADULT,
              genres=None),
         genres.Fantasy,
         genres.Science_Fiction,
         genres.Historical_Fiction,
         genres.Cooking,
         dict(name="Romance",
              genres=[genres.Romance_Erotica],
          ),
         genres.Science_Technology_Nature,
         genres.Self_Help,
         genres.Graphic_Novels_Comics,
         genres.Reference,
         dict(
             name="Young Adult",
             fiction=Lane.BOTH_FICTION_AND_NONFICTION,
             audience=genres.Classifier.AUDIENCE_YOUNG_ADULT,
             genres=[]),
         dict(
             name="Children",
             fiction=Lane.BOTH_FICTION_AND_NONFICTION,
             audience=genres.Classifier.AUDIENCE_CHILDREN,
             genres=[]),
     ]
    )
    Conf.initialize(_db, lanes)

templates = Environment(loader=PackageLoader("templates", "."))
app = Flask(__name__)
app.config['DEBUG'] = True
app.debug = True

DEFAULT_LANGUAGES = ['eng']

INVALID_CREDENTIALS_PROBLEM = "http://library-simplified.com/problem/credentials-invalid"
INVALID_CREDENTIALS_TITLE = "A valid library card barcode number and PIN are required."
EXPIRED_CREDENTIALS_PROBLEM = "http://library-simplified.com/problem/credentials-expired"
EXPIRED_CREDENTIALS_TITLE = "Your library card has expired. You need to renew it."
NO_AVAILABLE_LICENSE_PROBLEM = "http://library-simplified.com/problem/no-license"


def problem(type, title, status, detail=None, instance=None, headers={}):
    """Create a Response that includes a Problem Detail Document."""
    data = problem_detail.json(type, title, status, detail, instance)
    final_headers = { "Content-Type" : problem_detail.JSON_MEDIA_TYPE }
    final_headers.update(headers)
    return Response(data, status, headers)
    
def languages_for_request():
    return languages_from_accept(flask.request.accept_languages)

def languages_from_accept(accept_languages):
    languages = []
    for locale, quality in accept_languages:
        language = LanguageCodes.iso_639_2_for_locale(locale)
        if language:
            languages.append(language)
    if not languages:
        languages = DEFAULT_LANGUAGES
    return languages

def authenticated_patron(barcode, pin):
    """Look up the patron authenticated by the given barcode/pin.

    If there's a problem, return a 2-tuple (URI, title) for use in a
    Problem Detail Document.

    If there's no problem, return a Patron object.
    """
    patron = auth.authenticated_patron(Conf.db, barcode, pin)
    if not patron:
        return (INVALID_CREDENTIALS_PROBLEM,
                INVALID_CREDENTIALS_TITLE)

    # Okay, we know who they are and their PIN is valid. But maybe the
    # account has expired?
    if not patron.authorization_is_active:
        return (EXPIRED_CREDENTIALS_PROBLEM,
                EXPIRED_CREDENTIALS_TITLE)

    # No, apparently we're fine.
    return patron


def authenticate(uri, title):
    """Sends a 401 response that enables basic auth"""
    return problem(
        uri, title, 401,
        headers= { 'WWW-Authenticate' : 'Basic realm="Library card"'})

def requires_auth(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        header = flask.request.authorization
        if not header:
            # No credentials were provided.
            return authenticate(
                INVALID_CREDENTIALS_PROBLEM,
                INVALID_CREDENTIALS_TITLE)

        patron = authenticated_patron(header.username, header.password)
        if isinstance(patron, tuple):
            flask.request.patron = None
            return authenticate(*patron)
        else:
            flask.request.patron = patron
        return f(*args, **kwargs)
    return decorated


@app.route('/')
def index():    
    return redirect(url_for('.navigation_feed'))

@app.route('/lanes/')
def navigation_feed():
    languages = languages_for_request()
    feed = NavigationFeed.main_feed(Conf.lanes)

    feed.add_link(
        rel="search", 
        type="application/opensearchdescription+xml",
        href=url_for('lane_search', lane=None, _external=True))
    feed.add_link(
        rel="http://opds-spec.org/shelf", 
        href=url_for('active_loans', _external=True))
    return unicode(feed)

def lane_url(cls, lane, order=None):
    return url_for('feed', lane=lane.name, order=order, _external=True)

@app.route('/loans/')
@requires_auth
def active_loans():
    feed = AcquisitionFeed.active_loans_for(flask.request.patron)
    return unicode(feed)

@app.route('/lanes/<lane>')
def feed(lane):
    languages = languages_for_request()
    arg = flask.request.args.get
    order = arg('order', 'recommended')
    last_seen_id = arg('last_seen', None)

    lane = Conf.lanes.by_name[lane]

    search_link = dict(
        rel="search",
        type="application/opensearchdescription+xml",
        href=url_for('lane_search', lane=lane.name, _external=True))

    if order == 'recommended':
        feed = AcquisitionFeed.featured(Conf.db, languages, lane)
        feed.add_link(**search_link)
        return unicode(feed)

    if order == 'title':
        feed = WorkFeed(lane, languages, Work.title)
        title = "%s: By title" % lane.name
    elif order == 'author':
        feed = WorkFeed(lane, languages, Work.authors)
        title = "%s: By author" % lane.name
    else:
        return "I don't know how to order a feed by '%s'" % order

    size = arg('size', '50')
    try:
        size = int(size)
    except ValueError:
        return "Invalid size: %s" % size
    size = min(size, 100)

    last_work_seen = None
    last_id = arg('after', None)
    if last_id:
        try:
            last_id = int(last_id)
        except ValueError:
            return "Invalid work ID: %s" % last_id
        try:
            last_work_seen = Conf.db.query(Work).filter(Work.id==last_id).one()
        except NoResultFound:
            return "No such work id: %s" % last_id

    this_url = url_for('feed', lane=lane.name, order=order, _external=True)
    page = feed.page_query(Conf.db, last_work_seen, size).all()
    url_generator = lambda x : url_for(
        'feed', lane=lane.name, order=x, _external=True)

    opds_feed = AcquisitionFeed(Conf.db, title, this_url, page, url_generator)
    # Add a 'next' link if appropriate.
    if page and len(page) >= size:
        after = page[-1].id
        next_url = url_for(
            'feed', lane=lane.name, order=order, after=after, _external=True)
        opds_feed.add_link(rel="next", href=next_url)

    opds_feed.add_link(**search_link)
    return unicode(opds_feed)

@app.route('/search', defaults=dict(lane=None))
@app.route('/search/', defaults=dict(lane=None))
@app.route('/search/<lane>')
def lane_search(lane):
    languages = languages_for_request()
    query = flask.request.args.get('q')
    if lane:
        lane = Conf.lanes.by_name[lane]    
    else:
        # Create a synthetic Lane that includes absolutely everything.
        lane = Lane.everything(Conf.db)
    this_url = url_for('lane_search', lane=lane.name, _external=True)
    if not query:
        # Send the search form
        return OpenSearchDocument.for_lane(lane, this_url)
    # Run a search.
    

    results = lane.search(languages, query).limit(50)
    info = OpenSearchDocument.search_info(lane)
    opds_feed = AcquisitionFeed(
        Conf.db, info['name'], 
        this_url + "?q=" + urllib.quote(query),
        results)
    return unicode(opds_feed)

@app.route('/works/<data_source>/<identifier>/checkout')
@requires_auth
def checkout(data_source, identifier):

    # Turn source + identifier into a LicensePool
    source = DataSource.lookup(Conf.db, data_source)
    if source is None:
        return problem("No such data source!", 404)
    identifier_type = source.primary_identifier_type

    id_obj, ignore = WorkIdentifier.for_foreign_id(
        Conf.db, identifier_type, identifier, autocreate=False)
    if not id_obj:
        # TODO
        return problem(
            NO_AVAILABLE_LICENSE_PROBLEM, "I never heard of such a book.", 404)

    pool = id_obj.licensed_through
    if not pool:
        return problem(
            NO_AVAILABLE_LICENSE_PROBLEM, 
            "I don't have any licenses for that book.", 404)

    best_pool, best_link = pool.best_license_link
    if not best_link:
        return problem(
            NO_AVAILABLE_LICENSE_PROBLEM,
            "Sorry, couldn't find an available license.", 404)

    best_pool.loan_to(flask.request.patron)
    return redirect(URLRewriter.rewrite(best_link.href))

@app.route('/gutenberg_tree/<gutenberg_id>')
def gutenberg_tree(gutenberg_id):
    source = DataSource.lookup(Conf.db, DataSource.GUTENBERG)
    wid, ignore = WorkIdentifier.for_foreign_id(
        Conf.db, WorkIdentifier.GUTENBERG_ID, gutenberg_id, False)
    pool = Conf.db.query(LicensePool).filter(
        LicensePool.data_source==source).filter(
            LicensePool.identifier==wid).one()
    work = pool.work
    template = templates.get_template('work_dump.html')
    return template.render(work=work, len=len)

print __name__
if __name__ == '__main__':

    debug = True
    host = "0.0.0.0"
    app.run(debug=debug, host=host)
