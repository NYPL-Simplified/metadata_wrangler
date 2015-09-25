import datetime
import isbnlib
import json
import logging
from nose.tools import set_trace

from core.overdrive import (
    OverdriveAPI,
    OverdriveRepresentationExtractor,
)

from mirror import (
    CoverImageMirror
)
from core.coverage import (
    CoverageProvider,
)

from core.coverage import CoverageProvider
from core.model import (
    DataSource,
    Edition,
    Hyperlink,
    Identifier,
    Measurement,
    Representation,
    Subject,
)
from core.monitor import Monitor
from core.util import LanguageCodes

class OverdriveBibliographicMonitor(CoverageProvider):
    """Fill in bibliographic metadata for Overdrive records."""

    cls_log = logging.getLogger("Overdrive Bibliographic Monitor")

    def __init__(self, _db):
        self._db = _db
        self.overdrive = OverdriveAPI(self._db)
        self.input_source = DataSource.lookup(_db, DataSource.OVERDRIVE)
        self.output_source = DataSource.lookup(_db, DataSource.OVERDRIVE)
        super(OverdriveBibliographicMonitor, self).__init__(
            "Overdrive Bibliographic Monitor",
            self.input_source, self.output_source)

    def process_edition(self, edition):
        identifier = edition.primary_identifier
        info = self.overdrive.metadata_lookup(identifier)
        if info.get('errorCode') == 'NotFound':
            # TODO: We need to represent some kind of permanent failure.
            raise Exception("ID not recognized by Overdrive")
        metadata = OverdriveRepresentationExtractor.book_info_to_metadata(
            info
        )
        if not metadata:
            raise Exception("Could not extract metadata from Overdrive data: %r" % info)
        metadata.apply(edition)

    media_type_for_overdrive_type = {
        "ebook-pdf-adobe" : "application/pdf",
        "ebook-pdf-open" : "application/pdf",
        "ebook-epub-adobe" : "application/epub+zip",
        "ebook-epub-open" : "application/epub+zip",
    }
        
class OverdriveCoverImageMirror(CoverImageMirror):
    """Downloads images from Overdrive and writes them to disk."""

    DATA_SOURCE = DataSource.OVERDRIVE
