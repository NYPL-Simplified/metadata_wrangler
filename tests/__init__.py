from nose.tools import set_trace
import os
from model import (
    Base,
    Patron,
    SessionManager,
    get_one_or_create,
)
from sqlalchemy.orm.session import Session

class DBInfo(object):
    connection = None
    engine = None
    transaction = None

def setup():
    # Connect to the database and create the schema within a transaction
    engine, connection = SessionManager.initialize(os.environ['DATABASE_URL_TEST'])
    Base.metadata.drop_all(connection)
    Base.metadata.create_all(connection)
    DBInfo.engine = engine
    DBInfo.connection = connection
    DBInfo.transaction = connection.begin_nested()

    db = Session(DBInfo.connection)
    SessionManager.initialize_data(db)

    # Test data: Create the patron used by the dummy authentication
    # mechanism.
    get_one_or_create(db, Patron, authorization_identifier="200")
    db.commit()

    print "Connection is now %r" % DBInfo.connection
    print "Transaction is now %r" % DBInfo.transaction

def teardown():
    # Roll back the top level transaction and disconnect from the database
    DBInfo.transaction.rollback()
    DBInfo.connection.close()
    DBInfo.engine.dispose()
