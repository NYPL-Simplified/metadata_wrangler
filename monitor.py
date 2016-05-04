import isbnlib
import csv
import sys

from nose.tools import set_trace
from psycopg2.extras import NumericRange
from sqlalchemy import or_
from sqlalchemy.orm import (
    aliased,
)

from fast import (
    FASTNames,
    LCSHNames,
)

from core.config import Configuration
from core.overdrive import OverdriveBibliographicCoverageProvider
from core.threem import ThreeMBibliographicCoverageProvider
from core.classifier import Classifier
from core.metadata_layer import ReplacementPolicy
from core.monitor import (
    IdentifierResolutionMonitor as CoreIdentifierResolutionMonitor,
    SubjectAssignmentMonitor,
    IdentifierSweepMonitor,
    WorkSweepMonitor,
    ResolutionFailed,
)
from core.model import (
    DataSource,
    Edition,
    Equivalency,
    Identifier,
    LicensePool,
    PresentationCalculationPolicy,
    Subject,
    UnresolvedIdentifier,
    Work,
)

from core.s3 import S3Uploader
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

class IdentifierResolutionMonitor(CoreIdentifierResolutionMonitor):
    """Turn an UnresolvedIdentifier into an Edition with a LicensePool.

    Or (for ISBNs) just a bunch of Resources.
    """

    LICENSE_SOURCE_NOT_ACCESSIBLE = (
        "Could not access underlying license source over the network.")
    UNKNOWN_FAILURE = "Unknown failure."

    def __init__(self, _db):
        # Since we are the metadata wrangler, any Overdrive and 3M
        # resources we find, we mirror to S3.
        mirror = S3Uploader()

        # We're going to be aggressive about recalculating the presentation
        # for this work because either the work is currently not set up
        # at all, or something went wrong trying to set it up.
        presentation_calculation_policy = PresentationCalculationPolicy(
            regenerate_opds_entries=True,
            update_search_index=True
        )
        policy = ReplacementPolicy.from_metadata_source(
            mirror=mirror, even_if_not_apparently_updated=True,
            presentation_calculation_policy=presentation_calculation_policy
        )
        overdrive = OverdriveBibliographicCoverageProvider(
            _db, metadata_replacement_policy=policy
        )
        threem = ThreeMBibliographicCoverageProvider(
            _db, metadata_replacement_policy=policy
        )
        content_cafe = ContentCafeCoverageProvider(_db)
        content_server = ContentServerCoverageProvider(_db)
        oclc_classify = OCLCClassifyCoverageProvider(_db)

        optional = []
        required = [overdrive, threem, content_cafe, content_server, oclc_classify]

        super(IdentifierResolutionMonitor, self).__init__(
            _db, "Identifier Resolution Manager", interval_seconds=5,
            optional_coverage_providers = optional,
            required_coverage_providers = required,
        )

        self.viaf = VIAFClient(self._db)
        self.image_mirrors = {
            DataSource.THREEM : ThreeMCoverImageMirror(self._db),
            DataSource.OVERDRIVE : OverdriveCoverImageMirror(self._db)
        }
        self.image_scaler = ImageScaler(self._db, self.image_mirrors.values())
        self.oclc_linked_data = LinkedDataCoverageProvider(self._db)


    def pre_fetch_hook(self):
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

    def finalize(self, unresolved_identifier):
        identifier = unresolved_identifier.identifier
        self.resolve_equivalent_oclc_identifiers(identifier)
        if unresolved_identifier.identifier.type==Identifier.ISBN:
            # Currently we don't try to create Works for ISBNs,
            # we just make sure all the Resources associated with the
            # ISBN are properly handled. At this point, that has
            # completed successfully, so do nothing.
            pass
        else:
            self.process_work(unresolved_identifier)

    def process_work(self, unresolved_identifier):
        """Fill in VIAF data and cover images where possible before setting
        a previously-unresolved identifier's work as presentation ready."""

        work = None
        license_pool = unresolved_identifier.identifier.licensed_through
        if license_pool:
            work, created = license_pool.calculate_work(even_if_no_author=True)
        if work:
            self.resolve_viaf(work)
            self.resolve_cover_image(work)
            work.calculate_presentation()
            work.set_presentation_ready()
        else:
            raise ResolutionFailed(
                500,
                "Work could not be calculated for %r" % unresolved_identifier.identifier,
            )

    def resolve_equivalent_oclc_identifiers(self, identifier):
        """Ensures OCLC coverage for an identifier.

        This has to be called after the OCLCClassify coverage is run to confirm
        that equivalent OCLC identifiers are available.
        """
        oclc_ids = set()
        types = [Identifier.OCLC_WORK, Identifier.OCLC_NUMBER, Identifier.ISBN]
        for edition in identifier.primarily_identifies:
            oclc_ids = oclc_ids.union(
                edition.equivalent_identifiers(type=types)
            )
        for oclc_id in oclc_ids:
            self.log.info("Currently processing equivalent identifier: %r", oclc_id)
            self.oclc_linked_data.ensure_coverage(oclc_id)

    def resolve_viaf(self, work):
        """Get VIAF data on all contributors."""
        viaf = VIAFClient(self._db)
        for edition in work.editions:
            for contributor in edition.contributors:
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
