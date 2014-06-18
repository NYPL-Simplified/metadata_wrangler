import sys

from flask import Flask
import flask

from model import (
    SessionManager,
    WorkRecord,
    Work,
    )
from lane import Lane, Unclassified
from database_credentials import SERVER, MAIN_DB, CONFIG
from opds import OPDSFeed


db = SessionManager.session(SERVER, MAIN_DB)
app = Flask(__name__)

@app.route('/lanes/<languages>')
def navigation_feed(languages):
    return unicode(OPDSFeed.main_navigation_feed(db, languages))

@app.route('/lanes/<languages>/<lane>')
def feed(languages, lane):
    order = flask.request.args.get('order', 'recommended')
    if order == 'recommended':
        m = OPDSFeed.recommended_feed
    elif order == 'title':
        m = OPDSFeed.title_feed
    return unicode(m(db, languages, lane))


if __name__ == '__main__':

    debug = True
    if len(sys.argv) >= 2:
        debug = not (sys.argv[1] == 'production')

    if debug:
        host = "0.0.0.0"
    else:
        host = "10.128.36.26"

    CONFIG['base_url'] = "http://" + host + ":5000"

    app.run(debug=debug, host=host)
