import os
from core.model import (
    Work,
)
from core.scripts import (
    WorkProcessingScript,
    Script,
)
from presentation_ready import MakePresentationReadyMonitor
from gutenberg import OCLCMonitorForGutenberg
from appeal import AppealCalculator
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
    

class WorkAppealCalculationScript(WorkProcessingScript):

    def __init__(self, data_directory, *args, **kwargs):
        super(WorkAppealCalculationScript, self).__init__(*args, **kwargs)
        self.calculator = AppealCalculator(self.db, data_directory)

    def query_hook(self, q):
        if not self.force:
            q = q.filter(Work.primary_appeal==None)        
        return q

    def process_work(self, work):
        self.calculator.calculate_for_work(work)


class WorkPresentationCalculationScript(WorkProcessingScript):

    def process_work(self, work):
        work.calculate_presentation(
            choose_edition=False, classify=True, choose_summary=True,
            calculate_quality=True)

    def query_hook(self, q):
        if not self.force:
            q = q.filter(Work.fiction==None).filter(Work.audience==None)
        return q
