import datetime
import os
import sys
import site
import re

from nose.tools import (
    assert_raises_regexp,
    eq_,
    set_trace,
)

from model import (
    CirculationEvent,
    DataSource,
    LicensePool,
    WorkIdentifier,
    WorkRecord,
)

from tests.db import (
    setup_module, 
    teardown_module, 
    DatabaseTest,
)

class TestDataSource(DatabaseTest):

    def test_initial_data_sources(self):
        sources = [
            (x.name, x.offers_licenses, x.primary_identifier_type)
            for x in DataSource.well_known_sources(self._db)
        ]

        expect = [
            (DataSource.GUTENBERG, True, WorkIdentifier.GUTENBERG_ID),
            (DataSource.OVERDRIVE, True, WorkIdentifier.OVERDRIVE_ID),
            (DataSource.THREEM, True, WorkIdentifier.THREEM_ID),
            (DataSource.AXIS_360, True, WorkIdentifier.AXIS_360_ID),
            (DataSource.OCLC, False, WorkIdentifier.OCLC_WORK),
            (DataSource.WEB, True, WorkIdentifier.URI)
        ]
        eq_(set(sources), set(expect))

class TestWorkIdentifier(DatabaseTest):

    def test_for_foreign_id(self):
        identifier_type = WorkIdentifier.ISBN
        isbn = "3293000061"

        # Getting the data automatically creates a database record.
        identifier, was_new = WorkIdentifier.for_foreign_id(
            self._db, identifier_type, isbn)
        eq_(WorkIdentifier.ISBN, identifier.type)
        eq_(isbn, identifier.identifier)
        eq_(True, was_new)

        # If we get it again we get the same data, but it's no longer new.
        identifier2, was_new = WorkIdentifier.for_foreign_id(
            self._db, identifier_type, isbn)
        eq_(identifier, identifier2)
        eq_(False, was_new)

class TestWorkRecord(DatabaseTest):

    def test_for_foreign_id(self):
        """Verify we can get a data source's view of a foreign id."""
        data_source = DataSource.lookup(self._db, DataSource.GUTENBERG)
        id = "549"
        type = WorkIdentifier.GUTENBERG_ID

        record, was_new = WorkRecord.for_foreign_id(
            self._db, data_source, type, id)
        eq_(data_source, record.data_source)
        identifier = record.primary_identifier
        eq_(id, identifier.identifier)
        eq_(type, identifier.type)
        eq_(True, was_new)

        # We can get the same work record by providing only the name
        # of the data source.
        record, was_new = WorkRecord.for_foreign_id(
            self._db, DataSource.GUTENBERG, type, id)
        eq_(data_source, record.data_source)
        eq_(identifier, record.primary_identifier)
        eq_(False, was_new)
        

class TestLicensePool(DatabaseTest):

    def test_for_foreign_id(self):
        """Verify we can get a LicensePool for a data source and an 
        appropriate work identifier."""
        pool, was_new = LicensePool.for_foreign_id(
            self._db, DataSource.GUTENBERG, WorkIdentifier.GUTENBERG_ID, "541")
        eq_(True, was_new)
        eq_(DataSource.GUTENBERG, pool.data_source.name)
        eq_(WorkIdentifier.GUTENBERG_ID, pool.identifier.type)
        eq_("541", pool.identifier.identifier)
        

    def test_no_license_pool_for_data_source_that_offers_no_licenses(self):
        """OCLC doesn't offer licenses. It only provides metadata. We can get
        a WorkRecord for OCLC's view of a book, but we cannot get a
        LicensePool for OCLC's view of a book.
        """
        assert_raises_regexp(
            ValueError, 
            'Data source "OCLC Classify" does not offer licenses',
            LicensePool.for_foreign_id,
            self._db, DataSource.OCLC, "1015", 
            WorkIdentifier.OCLC_WORK)

    def test_no_license_pool_for_non_primary_identifier(self):
        """Overdrive offers licenses, but to get an Overdrive license pool for
        a book you must identify the book by Overdrive's primary
        identifier, not some other kind of identifier.
        """
        assert_raises_regexp(
            ValueError, 
            "License pools for data source 'Overdrive' are keyed to identifier type 'Overdrive ID' \(not 'ISBN', which was provided\)",
            LicensePool.for_foreign_id,
            self._db, DataSource.OVERDRIVE, WorkIdentifier.ISBN, "{1-2-3}")
            

class TestCirculationEvent(DatabaseTest):

    def _event_data(self, **kwargs):
        for k, default in (
                ("source", DataSource.OVERDRIVE),
                ("id_type", WorkIdentifier.OVERDRIVE_ID),
                ("start", datetime.datetime.utcnow()),
                ("type", CirculationEvent.LICENSE_ADD),
        ):
            kwargs.setdefault(k, default)
        if 'old_value' in kwargs and 'new_value' in kwargs:
            kwargs['delta'] = kwargs['new_value'] - kwargs['old_value']
        return kwargs

    def test_create_event_from_string(self):
        pass

    def test_create_event_from_dict(self):
        pass

    def test_new_title(self):

        # Here's a new title.
        data = self._event_data(
            source=DataSource.OVERDRIVE,
            id="{1-2-3}",
            type=CirculationEvent.LICENSE_ADD,
            old_value=0,
            new_value=2,
        )
        
        # Turn it into an event and see what happens.
        event, ignore = CirculationEvent.from_dict(self._db, data)

        # The event is associated with the correct data source.
        eq_(DataSource.OVERDRIVE, event.license_pool.data_source.name)

        # The event identifies a work by its ID plus the data source's
        # primary identifier.
        eq_(WorkIdentifier.OVERDRIVE_ID, event.license_pool.identifier.type)
        eq_("{1-2-3}", event.license_pool.identifier.identifier)

        # The number of licenses has been set to the new value.
        eq_(2, event.license_pool.licenses_owned)

    def test_update_from_event(self):

        generic_event = dict(
            source=DataSource.THREEM,
            id="a1d45",
        )

        # Here's a new title. Ten copies available.
        data = self._event_data(
            type=CirculationEvent.LICENSE_ADD,
            old_value=0,
            new_value=10,
            **generic_event
        )
        event, ignore = CirculationEvent.from_dict(self._db, data)
        pool = event.license_pool
        eq_(10, pool.licenses_available)

        # All ten copies get checked out.
        data = self._event_data(
            type=CirculationEvent.CHECKOUT,
            old_value=10,
            new_value=0,
            **generic_event
        )
        event, ignore = CirculationEvent.from_dict(self._db, data)
        eq_(0, pool.licenses_available)

        # Three patrons place holds.
        data = self._event_data(
            type=CirculationEvent.HOLD_PLACE,
            old_value=0,
            new_value=3,
            **generic_event
        )
        event, ignore = CirculationEvent.from_dict(self._db, data)
        eq_(0, pool.licenses_available)
        eq_(3, pool.patrons_in_hold_queue)

        # One patron leaves the hold queue.
        data = self._event_data(
            type=CirculationEvent.HOLD_RELEASE,
            old_value=3,
            new_value=2,
            **generic_event
        )
        event, ignore = CirculationEvent.from_dict(self._db, data)
        eq_(0, pool.licenses_available)
        eq_(2, pool.patrons_in_hold_queue)

        # One patron checks in the book.
        data = self._event_data(
            type=CirculationEvent.CHECKIN,
            old_value=0,
            new_value=1,
            **generic_event
        )
        event, ignore = CirculationEvent.from_dict(self._db, data)
        # This temporarily creates an inconsistent state.
        eq_(1, pool.licenses_available)
        eq_(2, pool.patrons_in_hold_queue)

        # But a signal is then sent to the next person in the queue
        # saying that the book is available.
        data = self._event_data(
            type=CirculationEvent.AVAILABILITY_NOTIFY,
            delta=1,
            **generic_event
        )
        event, ignore = CirculationEvent.from_dict(self._db, data)
        eq_(0, pool.licenses_available)
        eq_(1, pool.patrons_in_hold_queue)
        eq_(1, pool.licenses_reserved)

        # That person checks the book out.

        # TODO: at this point we run into a problem--without tracking
        # internal patron ID, we don't know whether a checkout is
        # caused by someone who has a reserved copy or someone who is
        # checking out from the general pool.
