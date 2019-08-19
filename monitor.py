import isbnlib
import csv
import sys

from nose.tools import set_trace
from psycopg2.extras import NumericRange
from sqlalchemy import or_
from sqlalchemy.orm import (
    aliased,
)

from core.config import Configuration
from core.classifier import Classifier
from core.monitor import (
    SubjectSweepMonitor,
    IdentifierSweepMonitor,
    WorkSweepMonitor,
)
from core.model import (
    DataSource,
    Equivalency,
    Identifier,
    LicensePool,
    Subject,
    Work,
)

from content_cafe import ContentCafeAPI


class FASTNameAssignmentMonitor(SubjectSweepMonitor):

    SERVICE_NAME = "FAST/LCSH Name Assignment Monitor"

    def __init__(self, _db, collection=None, fast=None, lcsh=None):
        self.fast = fast or dict()
        self.lcsh = lcsh or dict()
        super(FASTNameAssignmentMonitor, self).__init__(_db)

    def item_query(self):
        """Find only FAST or LCSH subjects with identifiers but no names."""
        qu = super(FASTNameAssignmentMonitor, self).item_query()
        qu = qu.filter(Subject.type.in_([Subject.FAST, Subject.LCSH]))
        qu = qu.filter(Subject.name == None)
        qu = qu.filter(Subject.identifier != None)
        return qu

    def process_item(self, subject):
        if not subject.identifier:
            return
        if subject.name:
            return
        if subject.type == Subject.FAST:
            subject.name = self.fast.get(subject.identifier)
        elif subject.type == Subject.LCSH:
            subject.name = self.lcsh.get(subject.identifier)

class ContentCafeDemandMeasurementSweep(IdentifierSweepMonitor):
    """Ensure that every ISBN directly associated with a commercial
    identifier has a recent demand measurement.

    :TODO: This misses a lot of ISBNs, since 3M and Axis ISBNs aren't
    directly associated with a commercial identifier.
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
