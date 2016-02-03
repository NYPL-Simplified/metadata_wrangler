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

class OverdriveCoverImageMirror(CoverImageMirror):
    """Downloads images from Overdrive and writes them to disk."""

    DATA_SOURCE = DataSource.OVERDRIVE
