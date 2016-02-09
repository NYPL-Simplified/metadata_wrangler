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

from core.threem import ThreeMAPI
from core.config import Configuration
from core.overdrive import (
    OverdriveAPI,
    OverdriveBibliographicCoverageProvider,
)
from core.threem import ThreeMBibliographicCoverageProvider
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
from core.coverage import CoverageFailure

from mirror import ImageScaler
from content_cafe import (
    ContentCafeCoverageProvider,
    ContentCafeAPI,
)
from content_server import ContentServerCoverageProvider
from gutenberg import OCLCClassifyCoverageProvider
from oclc import LinkedDataCoverageProvider
from overdrive import OverdriveCoverImageMirror
from threem import ThreeMCoverImageMirror
from viaf import VIAFClient

class IdentifierResolutionMonitor(Monitor):
    """Turn an UnresolvedIdentifier into an Edition with a LicensePool.

    Or (for ISBNs) just a bunch of Resources.
    """

    LICENSE_SOURCE_NOT_ACCESSIBLE = (
        "Could not access underlying license source over the network.")
    UNKNOWN_FAILURE = "Unknown failure."

    def __init__(self, _db):
        super(IdentifierResolutionMonitor, self).__init__(
            _db, "Identifier Resolution Manager", interval_seconds=5)

        self.viaf = VIAFClient(self._db)
        self.image_mirrors = {
            DataSource.THREEM : ThreeMCoverImageMirror(self._db),
            DataSource.OVERDRIVE : OverdriveCoverImageMirror(self._db)
        }
        self.image_scaler = ImageScaler(self._db, self.image_mirrors.values())
        self.oclc_linked_data = LinkedDataCoverageProvider(self._db)


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

    def fetch_unresolved_identifiers(self):
        now = datetime.datetime.utcnow()
        one_day_ago = now - datetime.timedelta(days=1)
        needs_processing = or_(
            UnresolvedIdentifier.exception==None,
            UnresolvedIdentifier.most_recent_attempt < one_day_ago)
        q = self._db.query(UnresolvedIdentifier).join(
            UnresolvedIdentifier.identifier).filter(needs_processing)
        count = q.count()
        self.log.info("%d unresolved identifiers", count)

        return q.order_by(func.random()).all()

    @property
    def providers(self):
        overdrive = OverdriveBibliographicCoverageProvider(self._db)
        threem = ThreeMBibliographicCoverageProvider(self._db)
        content_cafe = ContentCafeCoverageProvider(self._db)
        content_server = ContentServerCoverageProvider(self._db)
        oclc_classify = OCLCClassifyCoverageProvider(self._db)

        return [overdrive, threem, content_cafe, content_server, oclc_classify]

    def eligible_providers_for(self, identifier):
        return [provider for provider in self.providers if identifier.type in
                provider.input_identifier_types]

    def run_once(self, start, cutoff):
        self.create_missing_unresolved_identifiers()
        unresolved_identifiers = self.fetch_unresolved_identifiers()
        self.log.info(
            "Processing %i unresolved identifiers", len(unresolved_identifiers)
        )

        for unresolved_identifier in unresolved_identifiers:
            # Evaluate which providers this Identifier needs coverage from.
            identifier = unresolved_identifier.identifier
            eligible_providers = self.eligible_providers_for(identifer)
            self.log.info("Ensuring coverage for %r", identifier)
            self._log_providers(identifier, eligible_providers)

            # Goes through all relevant providers and tries to ensure coverage
            # tracking exceptions & failures as necessary.
            for provider in eligible_providers[:]:
                try:
                    record = provider.ensure_coverage(identifier, force=True)
                    if isinstance(record, CoverageFailure):
                        self.process_failure(
                            unresolved_identifier, record.exception
                        )
                    else:
                        # We're covered! Never think of this provider again.
                        eligible_providers.remove(provider)
                except Exception as e:
                    self.process_failure(
                        unresolved_identifier, traceback.format_exc()
                    )
            if eligible_providers:
                # This identifier is still unresolved. It's lacking coverage for
                # 1 or more providers its eligible for. Update its attempts and
                # exception message.
                now = datetime.datetime.utcnow()
                if not unresolved_identifier.exception:
                    unresolved_identifier.exception = self.UNKNOWN_FAILURE
                self.log.warn(
                    "Failure: %r %s", identifier,
                    unresolved_identifier.exception
                )
                self._log_providers(identifier, providers)
                unresolved_identifier.most_recent_attempt = now
                if not unresolved_identifier.first_attempt:
                    unresolved_identifier.first_attempt = now
            else:
                try:
                    self.resolve_equivalent_oclc_identifiers(identifier)
                    self.process_work(unresolved_identifier)
                except Exception as e:
                    process_failure(
                        unresolved_identifier, traceback.format_exc()
                    )
            self._db.commit()

    def _log_providers(self, identifier, providers):
        """Logs a list of coverage providers"""

        providers_str = ", ".join([p.service_name for p in providers])
        self.log.info(
            "%r requires coverage from: %s", identifier, providers_str
        )

    def process_work(self, unresolved_identifier):
        """Fill in VIAF data and cover images where possible before setting
        a previously-unresolved identifier's work as presentation ready."""

        work = None
        license_pool = unresolved_identifier.identifier.licensed_through
        if license_pool:
            work, created = license_pool.calculate_work(even_if_no_author=True)
        if work:
            self.resolve_viaf(work)
            self.resolve_cover(work)
            work.calculate_presentation()
            work.set_presentation_ready()
            self._db.delete(unresolved_identifier)
        else:
            exception = "Work could not be calculated for %r" % unresolved_identifier.identifier
            process_failure(unresolved_identifier, exception)

    def resolve_equivalent_oclc_identifiers(self, identifier):
        """Ensures OCLC coverage for an identifier.

        This has to be called after the OCLCClassify coverage is run to confirm
        that equivalent OCLC identifiers are available.
        """
        primary_edition = identifier.equivalencies
        oclc_ids = primary_edition.equivalent_identifiers(
            type=[Identifier.OCLC_WORK, Identifier.OCLC_NUMBER, Identifier.ISBN]
        )
        for oclc_id in oclc_ids:
            self.oclc_linked_data.ensure_coverage(oclc_id)

    def resolve_viaf(self, work):
        """Get VIAF data on all contributors."""
        viaf = VIAFClient(self._db)
        for edition in work.editions:
            for contributor in primary_edition.contributors:
                viaf.process_contributor(contributor)
                if not contributor.display_name:
                    contributor.family_name, contributor.display_name = (
                        contributor.default_names())

    def resolve_cover_image(self, work):
        """Make sure we have the cover for all editions."""
        for edition in work.editions:
            data_source_name = edition.data_source.name
            if data_source_name in self.image_mirrors:
                self.image_mirrors[data_source_name].mirror_edition(edition)
                self.image_scaler.scale_edition(edition)

    def process_failure(self, unresolved_identifier, exception):
        unresolved_identifier.status = 500
        unresolved_identifier.exception = exception
        self.log.error(
            "FAILURE on %s: %s",
            unresolved_identifier.identifier, exception
        )
        return unresolved_identifier

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
