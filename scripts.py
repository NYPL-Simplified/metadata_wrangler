import csv
from nose.tools import set_trace
import os
from core.model import (
    Edition,
    Work,
)
from sqlalchemy.sql.functions import func
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
)
from appeal import AppealCalculator
from viaf import VIAFClient
from core.util.permanent_work_id import WorkIDCalculator

class FillInVIAFAuthorNames(Script):

    """Normalize author names using data from VIAF."""

    def __init__(self, force=False):
        self.force = force

    def run(self):
        """Fill in all author names with information from VIAF."""
        VIAFClient(self._db).run(self.force)

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

class PermanentWorkIDStressTestGenerationScript(Script):
    """Generate a stress test to use as the benchmark for the permanent
    work ID generation algorithm.
    """

    def __init__(self, destination_file, test_size=20000):
        self.destination_file = destination_file
        self.out = open(self.destination_file, "w")
        self.writer = csv.writer(self.out)
        self.writer.writerow(["Original author", "Normalized author", "Original title", "Normalized title", "Format", "Permanent work ID"])
        self.test_size = test_size

    def run(self):
        for edition in self._db.query(Edition).order_by(func.random()).limit(
                self.test_size):
            self.process_edition(edition)
        self.out.close()

    def ready(self, x):
        if isinstance(x, unicode):
            return x.encode("utf8")
        elif x:
            return x
        else:
            return ''

    def write_row(self, original_author, normalized_author, original_title,
                  normalized_title, format):
        permanent_id = WorkIDCalculator.permanent_id(
            normalized_title, normalized_author, format)
        row = [original_author, normalized_author,
               original_title, normalized_title,
               format, permanent_id]
        self.writer.writerow(map(self.ready, row))

    def process_edition(self, edition):
        contributors = edition.author_contributors
        if contributors:
            primary_author = contributors[0]
            primary_author_name = primary_author.name
        else:
            primary_author_name = None
        author = WorkIDCalculator.normalize_author(primary_author_name)
        if edition.subtitle:
            original_title = edition.title + u": " + edition.subtitle
        else:
            original_title = edition.title
        title = WorkIDCalculator.normalize_title(original_title)
        self.write_row(primary_author_name, author, original_title, title,
                       "ebook")
