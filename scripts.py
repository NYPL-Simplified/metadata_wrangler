import csv
import sys
from nose.tools import set_trace
import os
from core.model import (
    Contribution,
    DataSource,
    Edition,
    Equivalency,
    Identifier,
    PresentationCalculationPolicy,
    Subject,
    UnresolvedIdentifier,
    Work,
)
from oclc import LinkedDataCoverageProvider
from sqlalchemy.sql.functions import func
from sqlalchemy.sql.expression import or_
from overdrive import OverdriveCoverImageMirror
from monitor import IdentifierResolutionMonitor
from mirror import ImageScaler
from threem import (
    ThreeMCoverImageMirror,
)
from gutenberg import OCLCClassifyCoverageProvider
from core.scripts import (
    Explain,
    WorkProcessingScript,
    Script,
    RunMonitorScript,
)
from viaf import VIAFClient
from core.util.permanent_work_id import WorkIDCalculator

class RunIdentifierResolutionMonitor(RunMonitorScript):
    """Run the identifier resolution monitor.

    Why not just use RunMonitorScript with the
    IdentifierResolutionMonitor?. This monitor is unique in that it
    makes sense to give it specific Identifiers to process, as a
    troubleshooting measure.

    So this subclass has some extra code to look for command-line
    identifiers, create UnresolvedIdentifiers for them, and process
    only those identifiers. If no command-line identifiers are
    provided this works exactly like RunMonitorScript.
    """

    def __init__(self):
        super(RunIdentifierResolutionMonitor, self).__init__(
            IdentifierResolutionMonitor
        )

    def run(self):
        # Explicitly create UnresolvedIdentifiers for any Identifiers
        # mentioned on the command line.
        identifiers = self.parse_identifier_list(self._db, sys.argv[1:])
        for identifier in identifiers:
            self.log.info(
                "Registering UnresolvedIdentifier for %r", identifier
            )
            ui, ignore = UnresolvedIdentifier.register(
                self._db, identifier, force=True
            )
            success = self.monitor.resolve_and_handle_result(ui)
            if success:
                self.log.info("Success: %r", identifier)
            else:
                self.log.info("Failure: %r", identifier)

        if not identifiers:
            # Run the IdentifierResolutionMonitor as per normal usage.
            super(RunIdentifierResolutionMonitor, self).run()
        

class FillInVIAFAuthorNames(Script):

    """Normalize author names using data from VIAF."""

    def __init__(self, force=False):
        self.force = force

    def run(self):
        """Fill in all author names with information from VIAF."""
        VIAFClient(self._db).run(self.force)


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
        # where s.type in ('Overdrive', '3M') 
        q = "select s.type as type, s.identifier as identifier, s.name as name, s.fiction as fiction, s.audience as audience, g.name as genre, count(i.id) as ct from subjects s left join classifications c on s.id=c.subject_id left join identifiers i on c.identifier_id=i.id left join genres g on s.genre_id=g.id group by s.type, s.identifier, s.name, s.fiction, s.audience, g.name order by ct desc;"
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


class RedoOCLC(Explain):

    def __init__(self):
        self.oclcld = DataSource.lookup(self._db, DataSource.OCLC_LINKED_DATA)
        self.coverage = LinkedDataCoverageProvider(self._db)

    def run(self):
        id_type, identifier = sys.argv[1:]
        identifier, ignore = Identifier.for_foreign_id(
            self._db, id_type, identifier
        )
        self.fix_identifier(identifier)

    def fix_identifier(self, primary_identifier):
        equivalent_ids = primary_identifier.equivalent_identifier_ids(
            levels=6, threshold=0)
        return self.fix_identifier_with_equivalents(primary_identifier, equivalent_ids)

    def fix_identifier_with_equivalents(self, primary_identifier, equivalent_ids):
        for edition in primary_identifier.primarily_identifies:
            print "BEFORE"
            self.explain(self._db, edition)
            print "-" * 80

        t1 = self._db.begin_nested()

        equivalencies = self._db.query(Equivalency).filter(
            Equivalency.data_source == self.oclcld).filter(
                Equivalency.input_id.in_(equivalent_ids)
            )
        print "DELETING %d" % equivalencies.count()
        for e in equivalencies:
            if e.strength == 0:
                print "DELETING %r" % e
            self._db.delete(e)
        t1.commit()

        self.coverage.process_item(primary_identifier)

        equivalent_ids = primary_identifier.equivalent_identifier_ids(
            levels=6, threshold=0)
        equivalencies = self._db.query(Equivalency).filter(
            Equivalency.data_source == self.oclcld).filter(
                Equivalency.input_id.in_(equivalent_ids),
            )

        for edition in primary_identifier.primarily_identifies:
            if edition.work:
                edition.work.calculate_presentation()
            self.explain(self._db, edition)
        print "I WOULD NOW EXPECT EVERYTHING TO BE FINE."


class RedoOCLCForThreeMScript(Script):

    def __init__(self, test_session=None):
        # Allows tests to run without db session overlap.
        if test_session:
            self._session = test_session
        self.input_data_source = DataSource.lookup(self._db, DataSource.OCLC_LINKED_DATA)
        self.coverage = LinkedDataCoverageProvider(self._db)
        self.oclc_classify = OCLCClassifyCoverageProvider(self._db)
        self.viaf = VIAFClient(self._db)

    def do_run(self):
        """Re-runs OCLC Linked Data coverage provider to get viafs. Fetches
        author information and recalculates presentation."""
        identifiers = self.fetch_authorless_threem_identifiers()
        self.delete_coverage_records(identifiers)
        self.ensure_isbn_identifier(identifiers)
        for identifier in identifiers:
            self.coverage.ensure_coverage(identifier)
            self.merge_contributors(identifier)
            # Recalculate everything so the contributors can be seen.
            for contributor in identifier.primary_edition.contributors:
                self.viaf.process_contributor(contributor)
            identifier.primary_edition.calculate_presentation()
            if identifier.licensed_through:
                identifier.licensed_through.calculate_work()

    def fetch_authorless_threem_identifiers(self):
        """Returns a list of ThreeM identifiers that don't have contributors"""
        qu = self._db.query(Identifier).join(Identifier.primarily_identifies)
        qu = qu.outerjoin(Edition.contributions).filter(Contribution.id==None)
        qu = qu.filter(Identifier.type == Identifier.THREEM_ID)
        return qu.all()

    def delete_coverage_records(self, identifiers):
        """Deletes existing OCLC Linked Data coverage records to re-run and
        capture author data"""
        t1 = self._db.begin_nested()

        for identifier in identifiers:
            for coverage_record in identifier.coverage_records:
                if coverage_record.data_source == self.input_data_source:
                    self._db.delete(coverage_record)

        t1.commit()

    def ensure_isbn_identifier(self, identifiers):
        """Runs OCLCClassify to get ISBN numbers if they're not available."""
        identifiers_without_isbn = []
        for identifier in identifiers:
            equivalencies = identifier.equivalencies
            equivalent_types = [eq.output.type for eq in equivalencies]
            if Identifier.ISBN not in equivalent_types:
                identifiers_without_isbn.append(identifier)

        for identifier in identifiers_without_isbn:
            self.oclc_classify.ensure_coverage(identifier)

    def merge_contributors(self, identifier):
        """Gives a ThreeM primary edition any contributors found via OCLC-LD"""
        qu = self._db.query(Identifier).join(Identifier.inbound_equivalencies)
        qu = qu.filter(or_(
            Identifier.type == Identifier.OCLC_WORK,
            Identifier.type == Identifier.OCLC_NUMBER
        )).filter(Equivalency.input_id == identifier.id)

        oclc_contributions = []
        for oclc_identifier in qu.all():
            editions = oclc_identifier.primarily_identifies
            for edition in editions:
                oclc_contributions += edition.contributions

        for contribution in oclc_contributions:
            for edition in identifier.primarily_identifies:
                edition.add_contributor(contribution.contributor, contribution.role)
