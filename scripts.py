import argparse
import base64
from collections import Counter
import csv
import datetime
import os
import sys
import unicodedata

from nose.tools import set_trace

from sqlalchemy.sql import select
from sqlalchemy.sql.functions import func
from sqlalchemy.sql.expression import or_

from canonicalize import AuthorNameCanonicalizer, MockAuthorNameCanonicalizer

from core.model import (
    Collection,
    Complaint,
    Contribution,
    Contributor,
    DataSource,
    Edition,
    Equivalency,
    Identifier,
    IntegrationClient,
    LicensePool,
    Timestamp,
    Work,
    get_one,
    production_session,
)

from core.scripts import (
    CheckContributorNamesInDB,
    DatabaseMigrationInitializationScript,
    Explain,
    IdentifierInputScript,
    WorkProcessingScript,
    Script,
    RunMonitorScript,
)
from core.util.permanent_work_id import WorkIDCalculator
from core.util.personal_names import contributor_name_match_ratio

from oclc.linked_data import LinkedDataCoverageProvider
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


class InstanceInitializationScript(DatabaseMigrationInitializationScript):

    """Initializes the database idempotently without raising an error.
    Intended for use with docker and SIMPLIFIED_DB_TASK=auto.
    """

    def run(self, cmd_args=None):
        existing_timestamp = get_one(self._db, Timestamp, service=self.name)
        if not existing_timestamp:
            super(InstanceInitializationScript, self).run(cmd_args=cmd_args)


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


class DashboardScript(Script):
    """A basic dashboard that tracks recently registered and recently
    processed identifiers.
    """

    def write(self, s=''):
        self.out.write(s + "\n")

    def do_run(self, output=sys.stdout):
        self.out = output

        # Within the past 24 hours, how many new LicensePools became
        # available? This represents new registrations coming in.
        qu = self._db.query(
            Identifier.type, func.count(func.distinct(LicensePool.id))
        )
        new_pools = qu.select_from(LicensePool).join(LicensePool.identifier)
        self.report_the_past(
            "New LicensePools (~registrations)", new_pools,
            LicensePool.availability_time
        )
        self.write()

        # Within the past 24 hours, how many Works were updated?
        # This represents work being done to achieve coverage.
        qu = self._db.query(Identifier.type, func.count(func.distinct(Work.id)))
        updated_works = qu.select_from(Work).join(Work.license_pools).join(
            LicensePool.identifier
        )
        self.report_the_past(
            "Updated Works (~coverage)", updated_works, Work.last_update_time
        )
        self.write()

        # For each catalog, how many Identifiers have Works and how
        # many don't? This is a rough proxy for 'the basic tasks have
        # been done and we can improve the data at our leisure.'
        self.write("Current coverage:")
        total_done = Counter()
        total_not_done = Counter()
        types = set()
        for collection in self._db.query(Collection).order_by(Collection.id):
            done, not_done = self.report_backlog(collection)
            for type, count in done.items():
                total_done[type] += count
                types.add(type)
            for type, count in not_done.items():
                total_not_done[type] += count
                types.add(type)
        self.write("\n Totals:")
        self.report_backlog(None)

    def report_the_past(self, title, base_qu, field, days=7):
        """Go backwards `days` days into the past and execute a
        query for each day.
        """
        end = datetime.datetime.utcnow()
        one_day = datetime.timedelta(days=1)
        self.write("=" * len(title))
        self.write(title)
        self.write("=" * len(title))
        for i in range(days):
            start = end - one_day
            qu = base_qu.filter(field > start).filter(field <= end)
            qu = qu.order_by(Identifier.type)
            qu = qu.group_by(Identifier.type)
            def format(d):
                return d.strftime("%Y-%m-%d")
            print format(start)
            for count, type in qu:
                self.write(" %s - %s" % (type, count))
            end = start

    def decode_metadata_identifier(self, collection):
        """Decode a Collection's name into the parts used
        on the origin server to generate the origin Collection's
        metadata identifier.

        TODO: This could go into Collection. It's metadata-wrangler
        specific but this probably isn't the only place we'll need it.
        """
        try:
            combined = base64.decodestring(collection.name)
            return map(base64.decodestring, combined.split(':', 2))
        except Exception, e:
            try:
                unique_id = collection.unique_account_id
            except Exception, e:
                unique_id = None

        # Just show it as-is.
        return collection.name, unique_id

    def report_backlog_item(self, type, done, not_done):
        """Report what percentage of items of the given type have
        been processed.
        """
        done_for_type = done[type]
        not_done_for_type = not_done[type]
        total_for_type = done_for_type + not_done_for_type
        percentage_complete = (float(done_for_type) / total_for_type) * 100
        print "  %s %d/%d (%d%%)" % (
            type, done_for_type, total_for_type, percentage_complete
        )

    def report_backlog(self, collection):
        done = Counter()
        not_done = Counter()
        clause = LicensePool.work_id==None

        types = set()
        for clause, counter in (
                (LicensePool.work_id!=None, done),
                (LicensePool.work_id==None, not_done),
        ):
            qu = self._db.query(
                Identifier.type,
                func.count(func.distinct(Identifier.id)),
            ).select_from(
                Collection
            ).join(
                Collection.catalog
            ).outerjoin(
                Identifier.licensed_through
            )
            if collection:
                qu = qu.filter(
                    Collection.id==collection.id
                )
            qu = qu.filter(
                clause
            ).group_by(Identifier.type).order_by(Identifier.type)
            for type, count in qu:
                counter[type] += count
                types.add(type)
            if len(done) == 0 and len(not_done) == 0:
                # This catalog is empty.
                return done, not_done
        if collection:
            name, identifier = self.decode_metadata_identifier(collection)
            self.write(' %s/%s' % (name, identifier))
        for type in sorted(types):
            self.report_backlog_item(type, done, not_done)
        return done, not_done

