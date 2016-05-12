from nose.tools import set_trace, eq_
from . import DatabaseTest
from ..scripts import RedoOCLCForThreeMScript
from ..core.model import (
    Identifier,
    DataSource,
    CoverageRecord,
)

class DummyCoverageProvider(object):
    hit_count = 0

    def ensure_coverage(self, identifier):
        self.hit_count += 1

class TestRedoOCLCForThreeM(DatabaseTest):

    def setup(self):
        super(TestRedoOCLCForThreeM, self).setup()
        self.script = RedoOCLCForThreeMScript(self._db)

        self.edition1, lp = self._edition(
            data_source_name = DataSource.THREEM,
            identifier_type = Identifier.THREEM_ID,
            with_license_pool = True,
            title = "Metropolis"
        )

        self.edition2, lp = self._edition(
            data_source_name = DataSource.THREEM,
            identifier_type = Identifier.THREEM_ID,
            with_license_pool = True,
            title = "The ArchAndroid"
        )
        # Give edition2 a coverage record.
        self._coverage_record(self.edition2, self.script.input_data_source)

        # Create a control case.
        self.edition3, lp = self._edition(
            data_source_name = DataSource.THREEM,
            identifier_type = Identifier.THREEM_ID,
            with_license_pool = True,
            title = "The Electric Lady"
        )
        self._db.commit()

        # Remove contributors for the first two editions.
        contributions = self.edition1.contributions + self.edition2.contributions
        contributors = self.edition1.contributors + self.edition2.contributors
        for c in contributions + contributions:
            self._db.delete(c)
        self._db.commit()

    def test_fetch_authorless_threem_identifiers(self):
        identifiers = self.script.fetch_authorless_threem_identifiers()

        # Both the editions with and without coverage records are selected...
        eq_(2, len(identifiers))
        # ...while the edition with contributors is not.
        assert self.edition3.primary_identifier not in identifiers

    def test_delete_coverage_records(self):
        oclc = DataSource.lookup(self._db, DataSource.OCLC_LINKED_DATA)
        q = self._db.query(CoverageRecord).filter(
            CoverageRecord.data_source==oclc
        )
        coverage_records_before = q.all()
        eq_(1, len(coverage_records_before))
        eq_(self.edition2.primary_identifier, coverage_records_before[0].identifier)

        identifiers = [self.edition1.primary_identifier, self.edition2.primary_identifier]
        self.script.delete_coverage_records(identifiers)
        coverage_records_after = q.all()
        eq_(0, len(coverage_records_after))

    def test_ensure_isbn_identifier(self):
        self.script.oclc_classify = DummyCoverageProvider()
        eq_(0, self.script.oclc_classify.hit_count)

        # When there are no equivalent identifiers, both identifiers go to the
        # OCLCClassify coverage provider.
        identifiers = [self.edition1.primary_identifier, self.edition2.primary_identifier]
        self.script.ensure_isbn_identifier(identifiers)
        eq_(2, self.script.oclc_classify.hit_count)

        # If an edition already has an ISBN identifier it doesn't go to the
        # coverage provider.
        self.script.oclc_classify.hit_count = 0
        self.edition1.primary_identifier.equivalent_to(
            DataSource.lookup(self._db, DataSource.GUTENBERG),
            self._identifier(identifier_type = Identifier.ISBN), 1
        )
        self._db.commit()
        self.script.ensure_isbn_identifier(identifiers)
        eq_(1, self.script.oclc_classify.hit_count)

    def test_merge_contributors(self):
        oclc_work = self._identifier(identifier_type=Identifier.OCLC_WORK)
        oclc_number = self._identifier(identifier_type=Identifier.OCLC_NUMBER)
        for oclc_id in [oclc_work, oclc_number]:
            # Create editions for each OCLC Identifier, give them a contributor,
            # and set them equivalent.
            edition = self._edition(
                data_source_name = self.script.input_data_source.name,
                identifier_type = oclc_id.type,
                identifier_id = oclc_id.identifier,
                title = "King Kong Ain't Got Nothin On Me"
            )
            edition.contributors[0].name = "Denzel Washington"
            self.edition1.primary_identifier.equivalent_to(
                self.script.input_data_source, oclc_id, 1
            )
            self._db.commit()
        eq_(0, len(self.edition1.contributors))
        self.script.merge_contributors(self.edition1.primary_identifier)
        eq_(2, len(self.edition1.contributors))
        eq_("Denzel Washington", self.edition1.contributors[0].name)
