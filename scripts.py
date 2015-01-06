import os
from core.scripts import Script
from presentation_ready import MakePresentationReadyMonitor
from gutenberg import OCLCMonitorForGutenberg

from viaf import VIAFClient


class MakePresentationReady(Script):

    def run(self):
        """Find all Works that are not presentation ready, and make them
        presentation ready.
        """
        MakePresentationReadyMonitor(os.environ['DATA_DIRECTORY']).run(
            self._db)


class FillInVIAFAuthorNames(Script):

    """Normalize author names using data from VIAF."""

    def __init__(self, force=False):
        self.force = force

    def run(self):
        """Fill in all author names with information from VIAF."""
        VIAFClient(self._db).run(self.force)


class OCLCMonitorForGutenbergScript(Script):

    def run(self):
        OCLCMonitorForGutenberg(self._db).run()
    
