import isbnlib
import csv
import datetime
import os
import requests
import sys
import traceback

from nose.tools import set_trace
from psycopg2.extras import NumericRange
from sqlalchemy import or_
from sqlalchemy.sql.functions import func
from sqlalchemy.orm import (
    aliased,
)

from fast import (
    FASTNames,
    LCSHNames,
)

from threem import ThreeMAPI
from core.config import Configuration
from core.overdrive import OverdriveAPI
from core.opds_import import SimplifiedOPDSLookup
from core.classifier import Classifier
from core.monitor import (
    Monitor,
    PresentationReadyMonitor,
    SubjectAssignmentMonitor,
    IdentifierSweepMonitor,
    WorkSweepMonitor,
)
from core.model import (
    DataSource,
    Edition,
    Equivalency,
    Identifier,
    LicensePool,
    Subject,
    UnresolvedIdentifier,
    Work,
)
from core.opds_import import DetailedOPDSImporter

from mirror import ImageScaler
from content_cafe import (
    ContentCafeCoverageProvider,
    ContentCafeAPI,
)
from overdrive import (
    OverdriveBibliographicMonitor,
    OverdriveCoverImageMirror,
)
from threem import (
    ThreeMBibliographicMonitor,
    ThreeMCoverImageMirror,
)

from gutenberg import (
    OCLCClassifyMonitor,
    OCLCMonitorForGutenberg,
)
from content_cafe import ContentCafeAPI
from oclc import LinkedDataCoverageProvider
from viaf import VIAFClient

class IdentifierResolutionMonitor(Monitor):
    """Turn an UnresolvedIdentifier into an Edition with a LicensePool.

    Or (for ISBNs) just a bunch of Resources.
    """

    LICENSE_SOURCE_RETURNED_ERROR = "Underlying license source returned error."
    LICENSE_SOURCE_RETURNED_WRONG_CONTENT_TYPE = (
        "Underlying license source served unhandlable media type (%s).")
    LICENSE_SOURCE_NOT_ACCESSIBLE = (
        "Could not access underlying license source over the network.")
    UNKNOWN_FAILURE = "Unknown failure."

    def __init__(self, _db):
        super(IdentifierResolutionMonitor, self).__init__(
            _db, "Identifier Resolution Manager", interval_seconds=5)
        content_server_url = Configuration.integration_url(
            Configuration.CONTENT_SERVER_INTEGRATION, required=True)
        self.content_server = SimplifiedOPDSLookup(content_server_url)
        self.overdrive = OverdriveAPI(self._db)
        self.threem = ThreeMAPI(self._db)

    def create_missing_unresolved_identifiers(self):
        """Find any Identifiers that should have LicensePools but don't,
        and also don't have an UnresolvedIdentifier record.

        Give each one an UnresolvedIdentifier record.

        This is a defensive measure.
        """
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
                        Edition.work_id==None)

        for q, msg, force in (
                (licensepool_but_no_edition, 
                 "Creating UnresolvedIdentifiers for %d incompletely resolved Identifiers (LicensePool but no Edition).", True),
                (no_work_because_of_missing_metadata, 
                 "Creating UnresolvedIdentifiers for %d Identifiers that have no Work because their Editions are missing title or author.", True),
                (seemingly_resolved_but_no_licensepool,
                 "Creating UnresolvedIdentifiers for %d identifiers missing both LicensePool and UnresolvedIdentifier.", False),
        ):

            count = q.count()
            if count:
                self.log.info(msg, count)
                for i in q:
                    UnresolvedIdentifier.register(self._db, i, force=force)
        

    def run_once(self, start, cutoff):
        self.create_missing_unresolved_identifiers()

        self.overdrive_coverage_provider = OverdriveBibliographicMonitor(self._db)
        self.threem_coverage_provider = ThreeMBibliographicMonitor(self._db)
        self.content_cafe_provider = ContentCafeCoverageProvider(self._db)


        providers_need_resolving = [
                (DataSource.GUTENBERG, None, self.resolve_content_server, None, 10),
                (DataSource.THREEM, None, self.resolve_through_coverage_provider, self.threem_coverage_provider, 25),
                (DataSource.OVERDRIVE, None, self.resolve_through_coverage_provider, self.overdrive_coverage_provider, 25),
                (DataSource.CONTENT_CAFE, Identifier.ISBN, self.resolve_identifiers_through_coverage_provider, self.content_cafe_provider, 25),
        ]


        while providers_need_resolving:
            providers_need_resolving = self.run_through_providers_once(
                providers_need_resolving)
            self._db.commit()

    def run_through_providers_once(self, providers):

        now = datetime.datetime.utcnow()
        one_day_ago = now - datetime.timedelta(days=1)
        needs_processing = or_(
            UnresolvedIdentifier.exception==None,
            UnresolvedIdentifier.most_recent_attempt < one_day_ago)

        new_providers = []
        for provider in providers:
            (data_source_name, identifier_type, handler, arg, 
             batch_size) = provider
            complete_server_failure = False
            data_source = DataSource.lookup(self._db, data_source_name)
            identifier_type = (
                identifier_type or data_source.primary_identifier_type)

            q = self._db.query(UnresolvedIdentifier).join(
                UnresolvedIdentifier.identifier).filter(
                    Identifier.type==identifier_type).filter(
                        needs_processing)
            count = q.count()               
            self.log.info(
                "%d unresolved identifiers of type %s",
                count, identifier_type
            )
            unresolved_identifiers = q.order_by(func.random()).limit(
                batch_size).all()
            self.log.info(
                "Handling %d unresolved identifiers of type %s.", 
                len(unresolved_identifiers), identifier_type
            )
            successes, failures = handler(
                unresolved_identifiers, data_source, arg)
            if isinstance(successes, int):
                # There was a problem getting any information at all from
                # the server.
                self.log.error(
                    "Got unexpected response code %d", successes)
                if successes / 100 == 5:
                    # A 5xx error means we probably won't get any
                    # other information from the server for a
                    # while. Give up on this server for now.
                    complete_server_failure = True

                # Some other kind of error means we might have
                # better luck if we choose different identifiers,
                # so keep going.
                successes = failures = []
            self.log.info(
                "%d successes, %d failures.",
                len(successes), len(failures)
            )
            for s in successes:
                self.log.info("Success: %r", s.identifier)
                self._db.delete(s)
            for f in failures:
                if not f.exception:
                    f.exception = self.UNKNOWN_FAILURE
                self.log.warn("Failure: %r %r", f.identifier, f.exception)
                f.most_recent_attempt = now
                if not f.first_attempt:
                    f.first_attempt = now
            if not complete_server_failure and count > batch_size:
                # Theres' more work to be done.
                new_providers.append(provider)
        return new_providers

    def resolve_content_server(self, batch, data_source, ignore):
        successes = []
        failures = []
        tasks_by_identifier = dict()
        for task in batch:
            tasks_by_identifier[task.identifier] = task
        try:
            identifiers = [x.identifier for x in batch]
            response = self.content_server.lookup(identifiers)
        except requests.exceptions.ConnectionError:
            return 500, self.LICENSE_SOURCE_NOT_ACCESSIBLE

        if response.status_code != 200:
            return response.status_code, self.LICENSE_SOURCE_RETURNED_ERROR

        content_type = response.headers['content-type']
        if not content_type.startswith("application/atom+xml"):
            return 500, self.LICENSE_SOURCE_RETURNED_WRONG_CONTENT_TYPE % (
                content_type)

        # We got an OPDS feed. Import it.
        importer = DetailedOPDSImporter(self._db, response.text)
        editions, messages = importer.import_from_feed()
        for edition in editions:
            identifier = edition.primary_identifier
            if identifier in tasks_by_identifier:
                # TODO: may need to uncomment this.
                edition.calculate_presentation()
                edition.license_pool.calculate_work(even_if_no_author=True)
                successes.append(tasks_by_identifier[identifier])
                del tasks_by_identifier[identifier]
        for identifier, (status_code, exception) in messages.items():
            if identifier not in tasks_by_identifier:
                # The server sent us a message about an identifier we
                # didn't ask for. No thanks.
                continue
            if status_code / 100 == 2:
                # The server sent us a 2xx status code for this
                # identifier but didn't actually give us any
                # information. That's a server-side problem.
                status_code == 500
            task = tasks_by_identifier[identifier]
            task.status_code = status_code
            task.exception = exception
            failures.append(task)
            del tasks_by_identifier[identifier]
        # Anything left in tasks_by_identifier wasn't mentioned
        # by the content server
        for identifier, task in tasks_by_identifier.items():
            task.status_code = 404
            task.exception = "Not mentioned by content server."
            failures.append(task)
        return successes, failures

    def resolve_through_coverage_provider(
            self, batch, data_source, coverage_provider):
        successes = []
        failures = []
        for task in batch:
            if self.resolve_one_through_coverage_provider(
                task, data_source, coverage_provider):
                successes.append(task)
            else:
                failures.append(task)
        return successes, failures

    def resolve_identifiers_through_coverage_provider(
            self, batch, data_source, coverage_provider):
        successes = []
        failures = []
        for task in batch:
            identifier = task.identifier
            start = datetime.datetime.now()
            try:
                coverage_provider.ensure_coverage(identifier, force=True)
                after_coverage = datetime.datetime.now()
                self.log.debug(
                    "Ensure coverage ran in %.2fs.",
                    (after_coverage-start).seconds
                )
                successes.append(task)
            except Exception, e:
                task.status_code = 500
                task.exception = traceback.format_exc()
                failures.append(task)
                self.log.error(
                    "FAILURE on %s: %r", task.identifier, e,
                    exc_info=e
                )
        return successes, failures

    def resolve_one_through_coverage_provider(
            self, task, data_source, coverage_provider):
        edition, is_new = Edition.for_foreign_id(
            self._db, data_source, task.identifier.type, task.identifier.identifier)
        license_pool, pool_is_new = LicensePool.for_foreign_id(
            self._db, data_source, task.identifier.type, task.identifier.identifier)
        start = datetime.datetime.now()
        try:
            coverage_provider.ensure_coverage(edition, force=True)
            after_coverage = datetime.datetime.now()
            edition.calculate_presentation()
            after_calculate_presentation = datetime.datetime.now()
            edition.license_pool.calculate_work(even_if_no_author=True)
            after_calculate_work = datetime.datetime.now()
            e1 = (after_coverage-start).seconds
            e2 = (after_calculate_presentation-after_coverage).seconds
            e3 = (after_calculate_work-after_calculate_presentation).seconds
            self.log.debug(
                "Ensure coverage ran in %.2fs. Calculate presentation ran in %.2fs. Calculate work ran in %.2fs.", 
                e1, e2, e3
            )
            return True
        except Exception, e:
            task.status_code = 500
            task.exception = traceback.format_exc()
            return False

class MetadataPresentationReadyMonitor(PresentationReadyMonitor):
    """Make works presentation ready.

    This is an EXTREMELY complicated process, but all the work can be
    delegated to other bits of code.
    """

    def __init__(self, _db, force=False):
        super(MetadataPresentationReadyMonitor, self).__init__(_db, [])
        self.data_directory = Configuration.data_directory()
        self.force = force

        self.threem_image_mirror = ThreeMCoverImageMirror(self._db)
        self.overdrive_image_mirror = OverdriveCoverImageMirror(self._db)
        self.image_mirrors = { DataSource.THREEM : self.threem_image_mirror,
                          DataSource.OVERDRIVE : self.overdrive_image_mirror }

        self.image_scaler = ImageScaler(
            self._db, self.image_mirrors.values())

        self.oclc_threem = OCLCClassifyMonitor(self._db, DataSource.THREEM)
        self.oclc_gutenberg = OCLCMonitorForGutenberg(self._db)
        self.oclc_linked_data = LinkedDataCoverageProvider(self._db)
        self.viaf = VIAFClient(self._db)

    def work_query(self):
        not_presentation_ready = or_(
            Work.presentation_ready==None,
            Work.presentation_ready==False)
        base = self._db.query(Work).filter(not_presentation_ready)
        # Uncommenting these lines will restrict to a certain type of
        # book.
        #
        #base = base.join(Work.editions).join(Edition.primary_identifier).filter(
        #                 Identifier.type!=Identifier.AXIS_360_ID)
        return base

    def process_batch(self, batch):
        biggest_id = 0
        for work in batch:
            if work.id > biggest_id:
                biggest_id = work.id
            try:
                self.process_work(work)
            except Exception, e:
                work.presentation_ready_exception = traceback.format_exc()
                self.log.error(
                    "ERROR MAKING WORK PRESENTATION READY: %s",
                    e, exc_info=e
                )
        self._db.commit()
        return biggest_id

    def process_work(self, work):
        start = datetime.datetime.now()
        if self.make_work_ready(work):
            after_work_ready = datetime.datetime.now()
            work.calculate_presentation()
            after_calculate_presentation = datetime.datetime.now()
            work.set_presentation_ready()
            self.log.info("NEW PRESENTATION READY WORK! %r", work)
            e1 = (after_work_ready-start).seconds
            e2 = (after_calculate_presentation-after_work_ready).seconds
            self.log.debug(
                "Make work ready took %.2fs. Calculate presentation took %.2fs.", e1, e2
            )
        else:
            self.log.error(
                "WORK STILL NOT PRESENTATION READY BUT NO EXCEPTION. WHAT GIVES?: %r", 
                work
            )

    def make_work_ready(self, work):
        """Either make a work presentation ready, or raise an exception
        explaining why that's not possible.
        """
        did_oclc_lookup = False
        oclc = LinkedDataCoverageProvider(self._db, processed_uris=set())

        for edition in work.editions:
            # OCLC Lookup on all Gutenberg editions.
            if edition.data_source.name==DataSource.GUTENBERG:
                if not self.oclc_gutenberg.ensure_coverage(edition):
                    # It's not a deal-breaker if we can't get OCLC
                    # coverage on an edition.
                    pass
                did_oclc_lookup = True
            elif edition.data_source.name==DataSource.THREEM:
                if not self.oclc_threem.ensure_coverage(edition):
                    # It's not a deal-breaker if we can't get OCLC
                    # coverage on an edition.
                    pass
                did_oclc_lookup = True

        primary_edition = work.primary_edition
        if did_oclc_lookup:
            oclc_ids = primary_edition.equivalent_identifiers(
                type=[Identifier.OCLC_WORK, Identifier.OCLC_NUMBER])
            # For a given edition, it's a waste of time to process a
            # given document from OCLC Linked Data more than once.
            for o in oclc_ids:
                oclc.ensure_coverage(o)

        # OCLC Linked Data on all ISBNs.
        equivalent_identifiers = primary_edition.equivalent_identifiers(
            type=[Identifier.ISBN])
        for identifier in equivalent_identifiers:
            oclc.ensure_coverage(identifier)

        # VIAF on all contributors.
        for edition in work.editions:
            for contributor in primary_edition.contributors:
                self.viaf.process_contributor(contributor)
                if not contributor.display_name:
                    contributor.family_name, contributor.display_name = (
                        contributor.default_names())

        # Make sure we have the cover for all editions.
        for edition in work.editions:
            n = edition.data_source.name
            if n in self.image_mirrors:
                self.image_mirrors[n].mirror_edition(edition)
                self.image_scaler.scale_edition(edition)

        # Success!
        return True

class FASTAwareSubjectAssignmentMonitor(SubjectAssignmentMonitor):

    def __init__(self, _db):
        data_dir = Configuration.data_directory()
        self.fast = FASTNames.from_data_directory(data_dir)
        self.lcsh = LCSHNames.from_data_directory(data_dir)
        self.fast = self.lcsh = {}
        super(FASTAwareSubjectAssignmentMonitor, self).__init__(_db)

    def process_batch(self, batch):
        for subject in batch:
            if subject.type == Subject.FAST and subject.identifier:
                subject.name = self.fast.get(subject.identifier, subject.name)
            elif subject.type == Subject.LCSH and subject.identifier:
                subject.name = self.lcsh.get(subject.identifier, subject.name)
        super(FASTAwareSubjectAssignmentMonitor, self).process_batch(batch)


class ContentCafeDemandMeasurementSweep(IdentifierSweepMonitor):
    """Ensure that every ISBN directly associated with a commercial
    identifier has a recent demand measurement.
    """

    def __init__(self, _db, batch_size=100, interval_seconds=3600*48):
        super(ContentCafeDemandMeasurementSweep, self).__init__(
            _db, 
            "Content Cafe demand measurement sweep", 
            interval_seconds)
        self.client = ContentCafeAPI(_db, mirror=None)
        self.batch_size = batch_size

    def identifier_query(self):
        # TODO: Outer join to Measurement. If measurement value is
        # None or less than a year old, skip it.
        input_identifier = aliased(Identifier)

        output_join_clause = Identifier.id==Equivalency.output_id
        input_join_clause = input_identifier.id==Equivalency.input_id

        qu = self._db.query(Identifier).join(
            Equivalency, output_join_clause).join(
                input_identifier, input_join_clause
            ).filter(Identifier.type==Identifier.ISBN).filter(
                input_identifier.type.in_(
                    [Identifier.OVERDRIVE_ID, Identifier.THREEM_ID, 
                     Identifier.AXIS_360_ID])
            ).order_by(Identifier.id)
        return qu

    def process_identifier(self, identifier):
        isbn = identifier.identifier
        if isbn and (isbnlib.is_isbn10(isbn) or isbnlib.is_isbn13(isbn)):
            self.client.measure_popularity(identifier, self.client.ONE_YEAR_AGO)
        return True


class ChildrensBooksWithNoAgeRangeMonitor(WorkSweepMonitor):

    def __init__(self, _db, batch_size=100, interval_seconds=600,
                 out=sys.stdout):
        super(ChildrensBooksWithNoAgeRangeMonitor, self).__init__(
            _db, 
            "Childrens' books with no age range", 
            interval_seconds)
        self.batch_size = batch_size
        self.out = csv.writer(out)

    def work_query(self):
        or_clause = or_(
            Work.target_age == None, 
            Work.target_age == NumericRange(None, None)
        )
        audiences = [
            Classifier.AUDIENCE_CHILDREN, 
            Classifier.AUDIENCE_YOUNG_ADULT
        ]
        qu = self._db.query(LicensePool).join(LicensePool.work).join(
            LicensePool.data_source).filter(
                DataSource.name != DataSource.GUTENBERG).filter(
                    Work.audience.in_(audiences)).filter(
                        or_clause
                    )
        return self._db.query(Work).filter(
            Work.audience.in_(audiences)).filter(
                or_clause
            )

    def process_work(self, work):
        for lp in work.license_pools:
            self.process_license_pool(work, lp)

    def process_license_pool(self, work, lp):
        identifier = lp.identifier
        axis = None
        overdrive = None
        threem = None
        gutenberg = None
        if identifier.type==Identifier.THREEM_ID:
            threem = identifier.identifier
        elif identifier.type==Identifier.OVERDRIVE_ID:
            overdrive = identifier.identifier
        elif identifier.type==Identifier.AXIS_360_ID:
            axis = identifier.identifier
        elif identifier.type==Identifier.GUTENBERG_ID:
            gutenberg = identifier.identifier
        isbns = [x.output.identifier for x in identifier.equivalencies
                 if x.output.type==Identifier.ISBN]
        if isbns:
            isbn = isbns[0]
        else:
            isbn = None
        data = [work.title.encode("utf8"), work.author.encode("utf8"), 
                work.audience, "", isbn, axis, overdrive, threem, gutenberg]
        self.out.writerow(data)
