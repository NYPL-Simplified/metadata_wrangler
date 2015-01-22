from nose.tools import set_trace
import os
from core.model import (
    Work,
)
from overdrive import OverdriveCoverImageMirror
from mirror import ImageScaler
from threem import (
    ThreeMCoverImageMirror,
)    

from core.scripts import (
    WorkProcessingScript,
    Script,
)
from amazon import AmazonCoverageProvider
from gutenberg import (
    GutenbergBookshelfClient,
    OCLCMonitorForGutenberg,
)
from appeal import AppealCalculator
from viaf import VIAFClient

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

class AmazonCoverageProviderScript(Script):

    def run(self):
        AmazonCoverageProvider(self._db).run()

class GutenbergBookshelfMonitorScript(Script):
    """Gather subject classifications and popularity measurements from
    Gutenberg's 'bookshelf' wiki.
    """
    def run(self):
        db = self._db
        GutenbergBookshelfClient(db).full_update()
        db.commit()

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

class CoverImageMirrorScript(Script):
    """This is not needed in normal usage, but it's useful to have it around
    in case the covers get screwed up."""
    
    def __init__(self, force=False, data_directory=None):
        self.force = force
        super(CoverImageMirrorScript, self).__init__()

    def run(self):
        ThreeMCoverImageMirror(self._db, self.data_directory).run()
        OverdriveCoverImageMirror(self._db, self.data_directory).run()


class CoverImageScaleScript(Script):
    """This is not needed in normal usage, but it's useful to have it around
    in case the covers get screwed up."""

    def __init__(self, force=False, data_directory=None):
        self.force = force
        super(CoverImageScaleScript, self).__init__()

    def run(self):
        mirrors = [OverdriveCoverImageMirror, ThreeMCoverImageMirror]
        ImageScaler(self._db, self.data_directory, mirrors).run(
            force=self.force)
