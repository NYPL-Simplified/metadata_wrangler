from testing import (
    DatabaseTest,
    _setup,
    _teardown,
)

class DBInfo(object):
    connection = None
    engine = None
    transaction = None

DatabaseTest.DBInfo = DBInfo

def setup():
    _setup(DBInfo)

def teardown():
    _teardown(DBInfo)
