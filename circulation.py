from nose.tools import set_trace
import sys

from sqlalchemy.orm.exc import (
    NoResultFound
)

import flask
from flask import Flask, url_for, redirect

from model import (
    DataSource,
    production_session,
    LicensePool,
    WorkIdentifier,
    Work,
    WorkFeed,
    )
from lane import Lane, Unclassified
from opds import (
    AcquisitionFeed,
    NavigationFeed,
    URLRewriter,
)

db = production_session()
app = Flask(__name__)
app.debug = True


@app.route('/')
def index():
    return redirect(url_for('.navigation_feed', languages='eng'))

@app.route('/lanes/<languages>')
def navigation_feed(languages):
    return unicode(NavigationFeed.main_feed(Lane, languages))

def lane_url(cls, languages, lane, order=None):
    if isinstance(lane, Lane):
        lane = lane.name
    if isinstance(languages, list):
        languages = ",".join(languages)

    return url_for('feed', languages=languages, lane=lane, order=order,
                   _external=True)


@app.route('/lanes/<languages>/<lane>')
def feed(languages, lane):

    languages = languages.split(",")

    arg = flask.request.args.get
    order = arg('order', 'recommended')
    last_seen_id = arg('last_seen', None)
    if order == 'recommended':
        return unicode(AcquisitionFeed.recommendations(db, languages, lane))

    if order == 'title':
        feed = WorkFeed(languages, lane, Work.title)
        title = "%s: By title" % lane
    elif order == 'author':
        feed = WorkFeed(languages, lane, Work.authors)
        title = "%s: By author" % lane
    else:
        return "I don't know how to order a feed by '%s'" % order

    size = arg('size', '50')
    try:
        size = int(size)
    except ValueError:
        return "Invalid size: %s" % size
    size = max(size, 10)
    size = min(size, 100)

    last_work_seen = None
    last_id = arg('after', None)
    if last_id:
        try:
            last_id = int(last_id)
        except ValueError:
            return "Invalid work ID: %s" % last_id
        try:
            last_work_seen = db.query(Work).filter(Work.id==last_id).one()
        except NoResultFound:
            return "No such work id: %s" % last_id

    language_key = ",".join(languages)
    this_url = url_for('feed', languages=language_key, lane=lane, order=order,
                       _external=True)
    page = feed.page_query(db, last_work_seen, size).all()
    opds_feed = AcquisitionFeed(db, title, this_url, page)
    # Add a 'next' link if appropriate.
    if page and len(page) >= size:
        after = page[-1].id
        next_url = url_for('feed', languages=language_key, 
                           lane=lane, order=order,
                           after=after, _external=True)
        opds_feed.links=[dict(rel="next", href=next_url)]
    return unicode(opds_feed)

@app.route('/works/<data_source>/<identifier>/checkout')
def checkout(data_source, identifier):

    # Turn source + identifier into a LicensePool
    source = DataSource.lookup(db, data_source)
    if source is None:
        return "No such data source!"
    identifier_type = source.primary_identifier_type

    id_obj, ignore = WorkIdentifier.for_foreign_id(
        db, identifier_type, identifier, autocreate=False)
    if not id_obj:
        # TODO
        return "I never heard of such a book."

    pool = id_obj.licensed_through
    if not pool:
        return "I don't have any licenses for that book."

    best_pool, best_link = pool.best_license_link
    if not best_link:
        return "Sorry, couldn't find an available license."
    
    return redirect(URLRewriter.rewrite(best_link))


print __name__
if __name__ == '__main__':

    debug = True
    host = "0.0.0.0"
    app.run(debug=debug, host=host)
