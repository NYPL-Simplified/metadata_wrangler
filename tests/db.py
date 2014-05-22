from sqlalchemy.engine import create_engine
from sqlalchemy.orm.session import Session
from model import (
    Base,
    SessionManager,
    DataSource,
)
from database_credentials import SERVER, TEST_DB

def setup_module():
    global transaction, connection, engine

    # Connect to the database and create the schema within a transaction
    engine, connection = SessionManager.initialize(SERVER, TEST_DB)
    transaction = connection.begin()
    Base.metadata.drop_all(connection)
    Base.metadata.create_all(connection)


def teardown_module():
    # Roll back the top level transaction and disconnect from the database
    transaction.rollback()
    connection.close()
    engine.dispose()


class DatabaseTest(object):
    def setup(self):
        self.__transaction = connection.begin_nested()
        self._db = Session(connection)
        SessionManager.initialize_data(self._db)

    def teardown(self):
        self._db.close()
        self.__transaction.rollback()
