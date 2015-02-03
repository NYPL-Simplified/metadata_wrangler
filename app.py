import os
from core.opds import VerboseAnnotator
from core.app_server import URNLookupController
from core.model import production_session

from flask import Flask

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

if __name__ == '__main__':

    debug = True
    host = "0.0.0.0"
    port = int(os.environ['METADATA_WEB_APP_PORT'])
    app.run(debug=debug, host=host, port=port)
