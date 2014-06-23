import sys

import flask
from flask import Flask, url_for, redirect

from model import (
    production_session,
    WorkRecord,
    Work,
    )
from lane import Lane, Unclassified
from opds import (
    AcquisitionFeed,
    NavigationFeed
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
    return "hey there."

if __name__ == '__main__':

    debug = True
    host = "0.0.0.0"
    app.run(debug=debug, host=host)
