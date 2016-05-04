import sys, os
from nose.tools import set_trace

from ..core.testing import (
    DatabaseTest,
    _setup,
    _teardown,
)

class MetadataDBInfo(object):
    connection = None
    engine = None
    transaction = None

DatabaseTest.DBInfo = MetadataDBInfo

def setup():
    _setup(MetadataDBInfo)

def teardown():
    _teardown(MetadataDBInfo)
