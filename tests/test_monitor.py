from nose.tools import set_trace, eq_
from . import DatabaseTest
from ..monitor import IdentifierResolutionMonitor
from ..core.model import Identifier

class DummyIdentifierResolutionMonitor(IdentifierResolutionMonitor):
    """An IdentifierRestolutionMonitor for testing"""

    def __init__(self):
        self.service_name = "Dummy Identifier Resolution Monitor"

    @property
    def providers(self):
        provider_types = [
            Identifier.ISBN, Identifier.OVERDRIVE_ID, Identifier.THREEM_ID,
            [Identifier.THREEM_ID, Identifier.ISBN],
            [Identifier.GUTENBERG_ID, Identifier.OCLC_WORK]
        ]
        return [DummyCoverageProvider(types) for types in provider_types]


class DummyCoverageProvider(object):
    def __init__(self, identifier_types):
        if not isinstance(identifier_types, list):
            identifier_types = [identifier_types]
        self.input_identifier_types = identifier_types
