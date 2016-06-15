import csv
import sys
from nose.tools import set_trace
from core.model import (
    Collection,
    Contribution,
    DataSource,
    Edition,
    Equivalency,
    Identifier,
    LicensePool,
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
    IdentifierInputScript,
    WorkProcessingScript,
    Script,
    RunMonitorScript,
)
from viaf import VIAFClient
from core.util.permanent_work_id import WorkIDCalculator

class RunIdentifierResolutionMonitor(RunMonitorScript, IdentifierInputScript):
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
        args = self.parse_command_line(self._db, autocreate=True)
        if args.identifiers:
            # Register specific UnresolvedIdentifiers, then resolve them.
            for identifier in args.identifiers:
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
        else:
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
        self.coverage = LinkedDataCoverageProvider(self._db)

    @property
    def oclcld(self):
        return DataSource.lookup(self._db, DataSource.OCLC_LINKED_DATA)

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
        self.coverage = LinkedDataCoverageProvider(self._db)
        self.oclc_classify = OCLCClassifyCoverageProvider(self._db)
        self.viaf = VIAFClient(self._db)

    @property
    def input_data_source(self):
        return DataSource.lookup(self._db, DataSource.OCLC_LINKED_DATA)

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


class CollectionGeneratorScript(Script):
    """Creates a new Collection object and prints client details to STDOUT"""

    def run(self, name):
        if not name:
            ValueError("No name provided. Could not create collection.")

        name = " ".join(name)
        print "Creating collection %s... " % name
        collection, plaintext_client_secret = Collection.register(self._db, name)

        print collection
        print ("RECORD THE FOLLOWING AUTHENTICATION DETAILS. "
               "The client secret cannot be recovered.")
        print "-" * 40
        print "CLIENT ID: %s" % collection.client_id
        print "CLIENT SECRET: %s" % plaintext_client_secret


class AddMissingUnresolvedIdentifiersScript(Script):

    """Find any Identifiers that should have LicensePools but don't,
    and also don't have an UnresolvedIdentifier record.

    Give each one an UnresolvedIdentifier record.
    
    This is a defensive measure.
    """

    def run(self):
        types = [Identifier.GUTENBERG_ID, Identifier.OVERDRIVE_ID,
                 Identifier.THREEM_ID, Identifier.AXIS_360_ID]

        # Find Identifiers that have LicensePools but no Editions and no
        # UnresolvedIdentifier.
        licensepool_but_no_edition = self._db.query(Identifier).join(Identifier.licensed_through).outerjoin(
            Identifier.primarily_identifies).outerjoin(
                Identifier.unresolved_identifier).filter(
                    Identifier.type.in_(types)).filter(
                        Edition.id==None).filter(UnresolvedIdentifier.id==None)

        # Identifiers that have no LicensePools and no UnresolvedIdentifier.
        seemingly_resolved_but_no_licensepool = self._db.query(Identifier).outerjoin(
            Identifier.licensed_through).outerjoin(
                Identifier.unresolved_identifier).filter(
                    Identifier.type.in_(types)).filter(
                        LicensePool.id==None).filter(UnresolvedIdentifier.id==None)

        # Identifiers whose Editions have no Work because they are
        # missing title, author or sort_author.
        no_title_or_author = or_(
            Edition.title==None, Edition.sort_author==None)
        no_work_because_of_missing_metadata = self._db.query(Identifier).join(
            Identifier.primarily_identifies).join(
                Identifier.licensed_through).filter(
                    no_title_or_author).filter(
                        LicensePool.work_id==None)

        for q, msg, force in (
                (licensepool_but_no_edition,
                 "Creating UnresolvedIdentifiers for %d incompletely resolved Identifiers (LicensePool but no Edition).", True),
                (no_work_because_of_missing_metadata,
                 "Creating UnresolvedIdentifiers for %d Identifiers that have no Work because their Editions are missing title or author.", True),
                (seemingly_resolved_but_no_licensepool,
                 "Creating UnresolvedIdentifiers for %d identifiers missing both LicensePool and UnresolvedIdentifier.", False),
        ):
            self.register_unresolved_identifiers(q, msg, force)

    def register_unresolved_identifiers(self, query, msg, force):
        """Register an UnresolvedIdentifier for every Identifier
        in `query`.

        :param qu: A query against `Identifier`
        :param msg: A message to log if any `Identifier`s match the query.
        :param force: Register an UnresolvedIdentifier even if the 
          identifier already has an associated LicensePool.
        """
        count = query.count()
        if count:
            self.log.info(msg, count)
            for i in query:
                UnresolvedIdentifier.register(self._db, i, force=force)
