import datetime
import os
import requests

from nose.tools import set_trace
from sqlalchemy import or_
from sqlalchemy.sql.functions import func

from threem import ThreeMAPI
from core.overdrive import OverdriveAPI
from core.opds_import import SimplifiedOPDSLookup
from core.monitor import (
    Monitor,
    PresentationReadyMonitor,
)
from core.model import (
    DataSource,
    Edition,
    Identifier,
    LicensePool,
    UnresolvedIdentifier,
    Work,
)
from core.opds_import import DetailedOPDSImporter

from mirror import ImageScaler
from overdrive import (
    OverdriveBibliographicMonitor,
    OverdriveCoverImageMirror,
)
from threem import (
    ThreeMBibliographicMonitor,
    ThreeMCoverImageMirror,
)

from appeal import AppealCalculator
from gutenberg import OCLCMonitorForGutenberg
from amazon import AmazonCoverageProvider
from oclc import LinkedDataCoverageProvider
from viaf import VIAFClient

class IdentifierResolutionMonitor(Monitor):
    """Turn an UnresolvedIdentifier into an Edition with a LicensePool."""

    LICENSE_SOURCE_RETURNED_ERROR = "Underlying license source returned error."
    LICENSE_SOURCE_RETURNED_WRONG_CONTENT_TYPE = (
        "Underlying license source served unhandlable media type (%s).")
    LICENSE_SOURCE_NOT_ACCESSIBLE = (
        "Could not access underlying license source over the network.")
    UNKNOWN_FAILURE = "Unknown failure."

    def __init__(self, _db):
        super(IdentifierResolutionMonitor, self).__init__(
            _db, "Identifier Resolution Manager", interval_seconds=5)
        content_server_url = os.environ['CONTENT_WEB_APP_URL']
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
                 Identifier.THREEM_ID]

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
                print msg % count
                for i in q:
                    UnresolvedIdentifier.register(self._db, i, force=force)
        

    def run_once(self, start, cutoff):
        self.create_missing_unresolved_identifiers()

        now = datetime.datetime.utcnow()
        one_day_ago = now - datetime.timedelta(days=1)
        needs_processing = or_(
            UnresolvedIdentifier.exception==None,
            UnresolvedIdentifier.most_recent_attempt < one_day_ago)

        overdrive_coverage_provider = OverdriveBibliographicMonitor(self._db)
        threem_coverage_provider = ThreeMBibliographicMonitor(self._db)

        for data_source_name, handler, arg, batch_size in (
                    (DataSource.GUTENBERG, self.resolve_content_server, None, 100),
                    (DataSource.THREEM, self.resolve_through_coverage_provider, threem_coverage_provider, 25),
                    (DataSource.OVERDRIVE, self.resolve_through_coverage_provider, overdrive_coverage_provider, 25),
        ):
            batches = 0
            data_source = DataSource.lookup(self._db, data_source_name)
            identifier_type = data_source.primary_identifier_type
            q = self._db.query(UnresolvedIdentifier).join(
                UnresolvedIdentifier.identifier).filter(
                    Identifier.type==identifier_type).filter(
                        needs_processing)
            print "%d unresolved identifiers of type %s" % (
                q.count(), identifier_type)
            while q.count() and batches < 10:
                batches += 1
                unresolved_identifiers = q.order_by(func.random()).limit(
                    batch_size).all()
                print "Handling %d unresolved identifiers." % len(
                    unresolved_identifiers) 
                successes, failures = handler(
                    unresolved_identifiers, data_source, arg)
                if isinstance(successes, int):
                    # There was a problem getting any information at all from
                    # the server.
                    print "Got unexpected response code %d" % successes
                    if successes / 100 == 5:
                        # A 5xx error means we probably won't get any
                        # other information from the server for a
                        # while. Give up on this server for now.
                        break

                    # Some other kind of error means we might have
                    # better luck if we choose different identifiers,
                    # so keep going.
                    successes = failures = []
                print "%d successes, %d failures." % (
                    len(successes), len(failures)
                )
                for s in successes:
                    print s.identifier
                    self._db.delete(s)
                for f in failures:
                    if not f.exception:
                        f.exception = self.UNKNOWN_FAILURE
                    print f.identifier, f.exception
                    f.most_recent_attempt = now
                    if not f.first_attempt:
                        f.first_attempt = now
                self._db.commit()
        self._db.commit()

    def resolve_content_server(self, batch, data_source, ignore):
        successes = []
        failures = []
        tasks_by_identifier = dict()
        for task in batch:
            tasks_by_identifier[task.identifier] = task
        try:
            response = self.content_server.lookup(
                [x.identifier for x in batch])
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

    def resolve_one_through_coverage_provider(
            self, task, data_source, coverage_provider):
        edition, is_new = Edition.for_foreign_id(
            self._db, data_source, task.identifier.type, task.identifier.identifier)
        license_pool, pool_is_new = LicensePool.for_foreign_id(
            self._db, data_source, task.identifier.type, task.identifier.identifier)
        try:
            coverage_provider.ensure_coverage(edition, force=True)
            edition.calculate_presentation()
            edition.license_pool.calculate_work(even_if_no_author=True)
            return True
        except Exception, e:
            task.status_code = 500
            task.exception = str(e)
            return False

class MetadataPresentationReadyMonitor(PresentationReadyMonitor):
    """Make works presentation ready.

    This is an EXTREMELY complicated process, but all the work can be
    delegated to other bits of code.
    """

    def __init__(self, _db, force=False):
        super(MetadataPresentationReadyMonitor, self).__init__(_db, [])
        self.data_directory = os.environ['DATA_DIRECTORY']
        self.force = force

        self.threem_image_mirror = ThreeMCoverImageMirror(self._db)
        self.overdrive_image_mirror = OverdriveCoverImageMirror(self._db)
        self.image_mirrors = { DataSource.THREEM : self.threem_image_mirror,
                          DataSource.OVERDRIVE : self.overdrive_image_mirror }

        self.image_scaler = ImageScaler(
            self._db, self.image_mirrors.values())

        self.appeal_calculator = AppealCalculator(self._db, self.data_directory)

        self.oclc_gutenberg = OCLCMonitorForGutenberg(self._db)
        self.oclc_linked_data = LinkedDataCoverageProvider(self._db)
        self.amazon = AmazonCoverageProvider(self._db)
        self.viaf = VIAFClient(self._db)

    def run_once(self, start, cutoff):

        not_presentation_ready = or_(
            Work.presentation_ready==None,
            Work.presentation_ready==False)

        base = self._db.query(Work).filter(not_presentation_ready)
        failed_works = base.filter(Work.presentation_ready_exception!=None)
        unready_works = base.filter(Work.presentation_ready_exception==None)
        print "%s works not presentation ready." % unready_works.count()
        print "%s works have presentation ready failures." % failed_works.count()
        for q in unready_works, failed_works:
            q = q.order_by(Work.last_update_time.desc())
            for work in q.all():
                try:
                    if self.make_work_ready(work):
                        work.calculate_presentation()
                        work.set_presentation_ready()
                        print "=NEW PRESENTATION READY WORK!="
                        print repr(work)
                        print "=============================="
                    else:
                        print "=WORK STILL NOT PRESENTATION READY BUT NO EXCEPTION. WHAT GIVES?="
                except Exception, e:
                    work.presentation_ready_exception = str(e)
                    print "=ERROR MAKING WORK PRESENTATION READY="
                    print e
                self._db.commit()

    def make_work_ready(self, work):
        """Either make a work presentation ready, or raise an exception
        explaining why that's not possible.
        """
        did_oclc_lookup = False
        for edition in work.editions:
            # OCLC Lookup on all Gutenberg editions.
            if edition.data_source.name==DataSource.GUTENBERG:
                if not self.oclc_gutenberg.ensure_coverage(edition):
                    # It's not a deal-breaker if we can't get OCLC
                    # coverage on an edition.
                    pass
                did_oclc_lookup = True

        primary_edition = work.primary_edition
        if did_oclc_lookup:
            oclc_ids = primary_edition.equivalent_identifiers(
                type=[Identifier.OCLC_WORK, Identifier.OCLC_NUMBER])
            for o in oclc_ids:
                self.oclc_linked_data.ensure_coverage(o)

        # OCLC Linked Data on all ISBNs.
        equivalent_identifiers = primary_edition.equivalent_identifiers(
            type=[Identifier.ISBN])
        for identifier in equivalent_identifiers:
            self.oclc_linked_data.ensure_coverage(identifier)

        # VIAF on all contributors.
        for edition in work.editions:
            for contributor in primary_edition.contributors:
                self.viaf.process_contributor(contributor)

        # Calculate appeal. This will obtain Amazon reviews as a side effect.
        self.appeal_calculator.calculate_for_work(work)

        # Make sure we have the cover for all editions.
        for edition in work.editions:
            n = edition.data_source.name
            if n in self.image_mirrors:
                self.image_mirrors[n].mirror_edition(edition)
                self.image_scaler.scale_edition(edition)

        # Success!
        return True
