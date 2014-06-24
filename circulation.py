import sys

import flask
from flask import Flask, url_for, redirect

from model import (
    DataSource,
    production_session,
    LicensePool,
    WorkIdentifier,
    Work,
    )
from lane import Lane, Unclassified
from opds import (
    AcquisitionFeed,
    NavigationFeed
)

print "LOADED"
db = production_session()
print "DB"
app = Flask(__name__)
app.debug = True
print "APP"
@app.route('/')
def index():
    return redirect(url_for('.navigation_feed', languages='eng'))

@app.route('/lanes/<languages>')
def navigation_feed(languages):
    return unicode(NavigationFeed.main_feed(Lane, languages))

@app.route('/lanes/<languages>/<lane>')
def feed(languages, lane):
    order = flask.request.args.get('order', 'recommended')
    if order == 'recommended':
        m = AcquisitionFeed.recommendations
    elif order == 'title':
        m = AcquisitionFeed.by_title
    elif order == 'author':
        m = AcquisitionFeed.by_author

    return unicode(m(db, languages, lane))

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
    
    return "Your book is at %s" % best_link
    #return redirect(link)


print "DONE"
print __name__
if __name__ == '__main__':

    debug = True
    host = "0.0.0.0"
    app.run(debug=debug, host=host)
