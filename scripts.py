import argparse
import csv
import datetime
import os
import sys
import unicodedata

from nose.tools import set_trace

from sqlalchemy.sql.functions import func
from sqlalchemy.sql.expression import or_

from canonicalize import AuthorNameCanonicalizer, MockAuthorNameCanonicalizer

from core.model import (
    get_one,
    Collection,
    Complaint,
    Contribution,
    Contributor, 
    DataSource,
    Edition,
    Equivalency,
    Identifier,
    IntegrationClient,
)

from core.scripts import (
    CheckContributorNamesInDB, 
    Explain,
    IdentifierInputScript,
    WorkProcessingScript,
    Script,
    RunMonitorScript,
)
from core.util.permanent_work_id import WorkIDCalculator
from core.util.personal_names import contributor_name_match_ratio

from mirror import ImageScaler
from oclc import LinkedDataCoverageProvider
from overdrive import OverdriveCoverImageMirror
from oclc_classify import OCLCClassifyCoverageProvider
from viaf import VIAFClient



class FillInVIAFAuthorNames(Script):

    """Normalize author names using data from VIAF."""

    def __init__(self, force=False):
        self.force = force

    def run(self):
        """Fill in all author names with information from VIAF."""
        VIAFClient(self._db).run(self.force)



class CheckContributorTitles(Script):
    """ For the Contributr objects in our database, goes to VIAF and extracts 
    titles (Mrs., Eminence, Prince, etc.) from the MARC records. 
    Output those titles to stdout.  Used to gather common name parts to help 
    hone the HumanName libraries. 
    """

    def __init__(self, viaf=None):
        self.viaf = viaf or VIAFClient(self._db)


    def run(self, batch_size=1000):
        """ 
        NOTE: We do not want to _db.commit in this method, as this script is look-no-touch. 
        """
        query = self._db.query(Contributor).filter(Contributor.viaf!=None).order_by(Contributor.id)

        if self.log:
            self.log.info(
                "Processing %d contributors.", query.count()
            )

        contributors = True
        offset = 0
        output = "ContributorID|\tSortName|\tTitle"
        print output.encode("utf8")
        from core.model import dump_query
        while contributors:
            my_query = query.offset(offset).limit(batch_size)

            print "query=%s" % dump_query(my_query)
            contributors = my_query.all()

            for contributor in contributors:
                self.process_contributor(contributor)
            offset += batch_size


    def process_contributor(self, contributor):
        if not contributor or not contributor.viaf:
            return

        # we should have enough known viaf ids for our task to only process those 
        contributor_titles = self.viaf.lookup_name_title(contributor.viaf)
        if contributor_titles:
            output = "%s|\t%s|\t%r" % (contributor.id, contributor.sort_name, contributor_titles)
            print output.encode("utf8")



class CheckContributorNamesOnWeb(CheckContributorNamesInDB):
    """
    Inherits process_contribution_local from parent.  
    Adds process_contribution_viaf functionality, which 
    sends a request to viaf to try and determine correct sort_name 
    for a given author.  
    """

    COMPLAINT_SOURCE = "CheckContributorNamesOnWeb"


    def __init__(self, _db=None, cmd_args=None):
        super(CheckContributorNamesOnWeb, self).__init__(_db=_db)

        parsed_args = self.parse_command_line(_db=self._db, cmd_args=cmd_args)
        self.mock_mode = parsed_args.mock

        if self.mock_mode:
            self.log.debug(
                "This is mocked run, with metadata coming from test files, rather than live OneClick connection."
            )
            self.base_path = os.path.split(__file__)[0]
            self.base_path = os.path.join(self.base_path, "tests")
            self.canonicalizer = MockAuthorNameCanonicalizer(self._db)
        else:
            self.canonicalizer = AuthorNameCanonicalizer(self._db)


    def run(self, batch_size=10):
        """
        TODO:  run the local db one, make a fix_mismatch, and 
        override it here.  in db local make it just register the complaint, 
        but here make it first check the web, then register the complaint.

        start by running the db local to make sure generated complaints where should
        then run the web search only on the ones that have complaints about.  either run only 
        on the non-
        """
        param_args = self.parse_command_line(self._db)
        
        self.query = self.make_query(
            self._db, param_args.identifier_type, param_args.identifiers, self.log
        )

        editions = True
        offset = 0
        output = "ContributorID|\tSortName|\tDisplayName|\tComputedSortName|\tResolution|\tComplaintSource"
        print output.encode("utf8")

        while editions:
            my_query = self.query.offset(offset).limit(batch_size)
            editions = my_query.all()

            for edition in editions:
                if edition.contributions:
                    for contribution in edition.contributions:
                        self.process_contribution_local(self._db, contribution, self.log)
            offset += batch_size

            self._db.commit()
        self._db.commit()


    @classmethod
    def arg_parser(cls):
        parser = super(CheckContributorNamesOnWeb, cls).arg_parser()

        parser.add_argument(
            '--mock', 
            help='If turned on, will use the MockCheckContributorNamesOnWeb client.', 
            action='store_true'
        )
        return parser


    def process_local_mismatch(self, _db, contribution, computed_sort_name, error_message_detail, log=None):
        """
        Overrides parent method to allow further resolution of sort_name problems by 
        calling process_contribution_web, which asks OCLC and VIAF for info. 
        Determines if a problem is to be investigated further or recorded as a Complaint, 
        to be solved by a human.  
        """ 
        self.process_contribution_web(_db=_db, contribution=contribution, 
            redo_complaints=False, log=log)


    def process_contribution_web(self, _db, contribution, redo_complaints=False, log=None):
        """
        If sort_name that got from VIAF is not too far off from sort_name we already have, 
        then use it (auto-fix).  If it is far off, then it's possible we did not match 
        the author very well.  Make a wrong-author complaint, and ask a human to fix it.

        Searches VIAF by contributor's display_name and contribution title.  If the 
        contributor already has a viaf_id store in our database, ignore it.  It's possible 
        that id was produced by an older, less precise matching algorithm and might want replacing. 

        :param redo_complaints: Should try OCLC/VIAF on the names that already have Complaint objects lodged against them?  
        Alternative is to require human review of all Complaints.
        """
        if not contribution or not contribution.edition:
            return

        contributor = contribution.contributor
        if not contributor.display_name:
            return

        identifier = contribution.edition.primary_identifier
        if not identifier:
            return

        known_titles = []
        if contribution.edition.title:
            known_titles.append(contribution.edition.title)

        # Searching viaf can be resource-expensive, so only do it if specifically asked
        # See if there are any complaints already lodged by a previous run of this script.
        pool = contribution.edition.is_presentation_for
        parent_source = super(CheckContributorNamesOnWeb, self).COMPLAINT_SOURCE
        complaint = get_one(
            _db, Complaint, on_multiple='interchangeable', 
            license_pool=pool, 
            source=self.COMPLAINT_SOURCE, 
            type=self.COMPLAINT_TYPE,
        )

        if not redo_complaints and complaint:
            # We already did some work on this contributor, and determined to 
            # ask a human for help.  This method was called with the time-saving 
            # redo_complaints=False flag.  Skip calling OCLC and VIAF.
            return

        # can we find an ISBN-type Identifier for this Contribution to send 
        # a request to OCLC with?
        isbn_identifier = None
        if identifier.type == Identifier.ISBN:
            isbn_identifier = identifier
        else:
            equivalencies = identifier.equivalencies
            for equivalency in equivalencies:
                if equivalency.output.type == Identifier.ISBN:
                    isbn_identifier = equivalency.output
                    break

        if isbn_identifier:
            # we can ask OCLC Linked Data about this ISBN
            uris = None
            sort_name, uris = self.canonicalizer.sort_name_from_oclc_linked_data(
                isbn_identifier, contributor.display_name)
            if sort_name:
                # see it's in correct format and not too far off from display_name
                name_ok = self.verify_sort_name(sort_name, contributor)
                if name_ok:
                    self.resolve_local_complaints(contribution)
                    self.set_contributor_sort_name(sort_name, contribution)
                    return
            else:
                # Nope. If OCLC Linked Data gave us any VIAF IDs, look them up
                # and see if we can get a sort name out of them.
                if uris:
                    for uri in uris:
                        match_found = self.canonicalizer.VIAF_ID.search(uri)
                        if match_found:
                            viaf_id = match_found.groups()[0]
                            contributor_data = self.canonicalizer.viaf.lookup_by_viaf(
                                viaf_id, working_display_name=contributor.display_name
                            )[0]
                            if contributor_data.sort_name:
                                # see it's in correct format and not too far off from display_name
                                name_ok = self.verify_sort_name(sort_name, contributor)
                                if name_ok:
                                    self.resolve_local_complaints(contribution)
                                    self.set_contributor_sort_name(sort_name, contribution)
                                    return

        # Nope. If we were given a display name, let's ask VIAF about it
        # and see what it says.
        sort_name = self.canonicalizer.sort_name_from_viaf(contributor.display_name, known_titles)
        if sort_name:
            # see it's in correct format and not too far off from display_name
            name_ok = self.verify_sort_name(sort_name, contributor)
            if name_ok:
                self.resolve_local_complaints(contribution)
                self.set_contributor_sort_name(sort_name, contribution)
                return

        # If we got to this point, we have not gotten a satisfying enough answer from 
        # either OCLC or VIAF.  Now is the time to generate a Complaint, ask a human to 
        # come fix this. 
        error_message_detail = "Contributor[id=%s].sort_name cannot be resolved from outside web services, human intervention required." % contributor.id
        self.register_problem(source=self.COMPLAINT_SOURCE, contribution=contribution, 
            computed_sort_name=sort_name, error_message_detail=error_message_detail, log=log)


    @classmethod
    def verify_sort_name(cls, sort_name, contributor):
        """
        See how well the new sort_name matches the display_name and the expected 'Last, First' format.
        Too far off is an unexpected result and is a problem.
        Does not check for proper formatting, like "Last, First".
        :return name_ok: Boolean answer to "is this computed name good enough?"
        """
        if not contributor.sort_name:
            # any port in a storm is an acceptable sort name
            return True

        computed_sort_name = unicodedata.normalize("NFKD", unicode(sort_name))

        if (contributor.sort_name.strip().lower() == computed_sort_name.strip().lower()):
            # no change is good change
            return True

        # computed names don't match.  by how much?  if it's a matter of a comma or a misplaced 
        # suffix, we can fix without asking for human intervention.  if the names are very different, 
        # there's a chance the sort and display names are different on purpose, s.a. when foreign names 
        # are passed as translated into only one of the fields, or when the author has a popular pseudonym. 
        # best ask a human.

        # if the relative lengths are off than by a stray space or comma, ask a human
        # it probably means that a human metadata professional had added an explanation/expansion to the 
        # sort_name, s.a. "Bob A. Jones" --> "Bob A. (Allan) Jones", and we'd rather not replace this data 
        # with the "Jones, Bob A." that the auto-algorigthm would generate.
        length_difference = len(contributor.sort_name.strip()) - len(computed_sort_name.strip())
        if abs(length_difference) > 3:
            return False

        match_ratio = contributor_name_match_ratio(contributor.sort_name, computed_sort_name, normalize_names=False)

        if (match_ratio < 40):
            # ask a human.  this kind of score can happen when the sort_name is a transliteration of the display_name, 
            # and is non-trivial to fix.  
            return False
        else:
            # we can fix it!
            return True


    def resolve_local_complaints(self, contribution):
        """
        Resolves any complaints that the parent script may have made about this 
        contributor's sort_name, because we've now asked the Web, and it gave us the answer. 
        """
        pool = contribution.edition.is_presentation_for
        parent_source = super(CheckContributorNamesOnWeb, self).COMPLAINT_SOURCE
        parent_type = super(CheckContributorNamesOnWeb, self).COMPLAINT_TYPE
        
        query = self._db.query(Complaint)
        query = query.filter(Complaint.license_pool_id == pool.id)
        query = query.filter(Complaint.source == parent_source)
        query = query.filter(Complaint.type == parent_type)
        query = query.filter(Complaint.resolved == None)

        complaints = query.all()
        for complaint in complaints:
            # say that we fixed it
            complaint.resolved = datetime.datetime.utcnow()



class CoverImageMirrorScript(Script):
    """This is not needed in normal usage, but it's useful to have it
    around in case the covers get screwed up, or to do intial
    bootstrapping of a large dataset.
    """

    def __init__(self, force=False):
        self.force = force
        super(CoverImageMirrorScript, self).__init__()

    def run(self):
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
        mirrors = [OverdriveCoverImageMirror]
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
            primary_author_name = primary_author.sort_name
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


class CatalogCategorizationOverviewScript(Script):

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


class IntegrationClientGeneratorScript(Script):

    """Creates a new IntegrationClient object and prints client details
    to STDOUT
    """

    def run(self, url):
        if not url:
            ValueError("No url provided. Could not create IntegrationClient.")

        url = " ".join(url)
        print "Creating IntegrationClient for '%s'" % url
        client, plaintext_secret = IntegrationClient.register(self._db, url)

        print client
        print ("RECORD THE FOLLOWING AUTHENTICATION DETAILS. "
               "The client secret cannot be recovered.")
        print "-" * 40
        print "CLIENT KEY: %s" % client.key
        print "CLIENT SECRET: %s" % plaintext_secret
        self._db.commit()
