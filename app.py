import sys

from flask import Flask
import flask

from model import (
    production_db,
    WorkRecord,
    Work,
    )
from lane import Lane, Unclassified
from opds import (
    AcquisitionFeed,
    NavigationFeed
)


db = production_db()
app = Flask(__name__)

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


if __name__ == '__main__':

    debug = False
    if len(sys.argv) >= 2:
        debug = not (sys.argv[1] == 'production')

    if debug:
        host = "0.0.0.0"
    else:
        host = "10.128.36.26"

    CONFIG['site']['root'] = "http://" + host + ":5000"

    app.run(debug=debug, host=host)
