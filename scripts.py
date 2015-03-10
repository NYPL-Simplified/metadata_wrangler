import csv
import sys
from nose.tools import set_trace
import os
from fast import (
    FASTNames,
    LCSHNames,
)
from core.model import (
    Edition,
    Subject,
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
    SubjectAssignmentScript,
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

class IdentifierResolutionScript(Script):


    def run(self):
        content_server_url = os.environ['CONTENT_WEB_APP_URL']
        content_server = SimplifiedOPDSLookup(content_server_url)
        overdrive = OverdriveAPI(self._db)
        threem = ThreeMAPI(self._db)
        IdentifierResolutionMonitor(content_server, overdrive, threem).run(
            self._db)


class CoverImageMirrorScript(Script):
    """This is not needed in normal usage, but it's useful to have it
    around in case the covers get screwed up, or to do intial
    bootstrapping of a large dataset.
    """
    
    def __init__(self, force=False):
        self.force = force
        super(CoverImageMirrorScript, self).__init__()

    def run(self):
        ThreeMCoverImageMirror(self._db).run()
        OverdriveCoverImageMirror(self._db).run()


class CoverImageScaleScript(Script):
    """This is not needed in normal usage, but it's useful to have it
    around in case the covers get screwed up, or to do initial
    bootstrapping of a large dataset.
    """

    def __init__(self, force=False):
        self.force = force
        super(CoverImageScaleScript, self).__init__()

    def run(self):
        mirrors = [OverdriveCoverImageMirror, ThreeMCoverImageMirror]
        ImageScaler(self._db, mirrors).run(force=self.force)

class PermanentWorkIDStressTestGenerationScript(Script):
    """Generate a stress test to use as the benchmark for the permanent
    work ID generation algorithm.
    """

    def __init__(self, destination_file):
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

    def write_row(self, title, author, normalized_title, normalized_author,
                  format):
        permanent_id = WorkIDCalculator.permanent_id(
            normalized_title, normalized_author, format)
        row = [title, author, normalized_title, normalized_author,
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

class CollectionCategorizationOverviewScript(Script):

    def __init__(self, output_path=None, cutoff=0):
        self.cutoff=cutoff
        if output_path:
            out = open(output_path, "w")
        else:
            out = sys.stdout
        self.writer = csv.writer(out)
        self.writer.writerow(
            ["Subject type", "Subject identifier", "Subject name",
             "Fiction", "Audience", "Genre"])

    def ready(self, x):
        if isinstance(x, unicode):
            return x.encode("utf8")
        elif x:
            return x
        else:
            return ''

    def run(self):
        q = "select s.type as type, s.identifier as identifier, s.name as name, s.fiction as fiction, s.audience as audience, g.name as genre, count(i.id) as ct from subjects s left join classifications c on s.id=c.subject_id left join identifiers i on c.identifier_id=i.id left join genres g on s.genre_id=g.id where s.type in ('Overdrive', '3M') group by s.type, s.identifier, s.name, s.fiction, s.audience, g.name order by ct desc;"
        q = self._db.query("type", "identifier", "name", "fiction", "audience", "genre", "ct").from_statement(q)
        for type, identifier, name, fiction, audience, genre, ct in q:
            if ct < self.cutoff:
                break
            if fiction == True:
                fiction = 'True'
            elif fiction == False:
                fiction = 'False'
            else:
                fiction = ''
            o = [type, identifier, name, fiction, audience, genre, ct]
            self.writer.writerow(map(self.ready, o))
                
class PermanentWorkIDStressTestScript(PermanentWorkIDStressTestGenerationScript):
    
    def __init__(self, input_path):
        self.input = open(input_path)
        self.reader = csv.reader(self.input)
        self.writer = csv.writer(sys.stdout)
        self.writer.writerow(["Title", "Author", "Normalized title", "Normalized author", "Format", "Permanent work ID"])

    def run(self):
        skipped = False
        wi = WorkIDCalculator
        for title, author, format in self.reader:
            if not skipped:
                skipped = True
                continue
            normalized_title = wi.normalize_title(title.decode("utf8"))
            normalized_author = wi.normalize_author(author.decode("utf8"))
            self.write_row(title, author, normalized_title, normalized_author, format)

class FASTAwareSubjectAssignmentScript(SubjectAssignmentScript):

    def __init__(self, force):
        data_dir = os.environ['DATA_DIRECTORY']
        self.fast = FASTNames.from_data_directory(data_dir)
        self.lcsh = LCSHNames.from_data_directory(data_dir)
        super(FASTAwareSubjectAssignmentScript, self).__init__(force)

    def process(self, subject):
        if subject.type == Subject.FAST and subject.identifier:
            subject.name = self.fast.get(subject.identifier, subject.name)
        elif subject.type == Subject.LCSH and subject.identifier:
            subject.name = self.lcsh.get(subject.identifier, subject.name)
        super(FASTAwareSubjectAssignmentScript, self).process(subject)
