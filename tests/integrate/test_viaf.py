import pkgutil
import StringIO
from integration.oclc import (
    OCLCXMLParser,
)
from nose.tools import set_trace, eq_

from model import (
    Contributor,
    )

from tests.db import (
    DatabaseTest,
)

from integration.viaf import (
    VIAFParser,
)

class TestVIAF(DatabaseText):


    
