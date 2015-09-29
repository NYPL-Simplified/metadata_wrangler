from nose.tools import set_trace

import isbnlib
import datetime
from lxml import etree

from mirror import CoverImageMirror
from core.coverage import CoverageProvider

from core.model import (
    Contributor,
    DataSource,
    Edition,
    Hyperlink,
    Identifier,
    Subject,
)
from core.coverage import CoverageProvider
from core.monitor import Monitor
from core.util.xmlparser import XMLParser
from core.threem import ThreeMAPI
from core.util import LanguageCodes


class ThreeMBibliographicMonitor(CoverageProvider):
    """Fill in bibliographic metadata for 3M records."""

    def __init__(self, _db,
                 account_id=None, library_id=None, account_key=None,
                 batch_size=1):
        self._db = _db
        self.api = ThreeMAPI(_db, account_id, library_id, account_key)
        self.input_source = DataSource.lookup(_db, DataSource.THREEM)
        self.output_source = DataSource.lookup(_db, DataSource.THREEM)
        super(ThreeMBibliographicMonitor, self).__init__(
            "3M Bibliographic Monitor",
            self.input_source, self.output_source)
        self.current_batch = []
        self.batch_size=batch_size

    def process_edition(self, edition):
        by_identifier = self.api.get_bibliographic_info_for([edition])
        [(edition, metadata)] = by_identifier.values()
        metadata.apply(
            edition,
            replace_identifiers=True,
            replace_subjects=True,
            replace_contributions=True,
            replace_links=True,
            replace_formats=True,
        )
        self.log.info("Processed edition %r", edition)
        return True
        

class ThreeMCoverImageMirror(CoverImageMirror):
    """Downloads images from 3M and writes them to disk."""

    DATA_SOURCE = DataSource.THREEM

    def filename_for(self, resource):
        return resource.identifier.identifier + ".jpg"
