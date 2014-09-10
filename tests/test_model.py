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
    Contributor,
    CoverageProvider,
    CoverageRecord,
    DataSource,
    Genre,
    Lane,
    LaneList,
    LicensePool,
    Timestamp,
    Work,
    WorkFeed,
    WorkIdentifier,
    WorkRecord,
    get_one_or_create,
)

import classifier
from classifier import (
    Classifier,
    Fantasy,
    Romance,
    Drama,
)

from tests.db import (
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

            (DataSource.OCLC, False, WorkIdentifier.OCLC_NUMBER),
            (DataSource.OCLC_LINKED_DATA, False, WorkIdentifier.OCLC_NUMBER),
            (DataSource.OPEN_LIBRARY, False, WorkIdentifier.OPEN_LIBRARY_ID),
            (DataSource.WEB, True, WorkIdentifier.URI),
            (DataSource.GUTENBERG_COVER_GENERATOR, False, WorkIdentifier.GUTENBERG_ID),
            (DataSource.CONTENT_CAFE, False, None),
            (DataSource.MANUAL, False, None)
        ]
        eq_(set(sources), set(expect))

    def test_lookup(self):
        gutenberg = DataSource.lookup(self._db, DataSource.GUTENBERG)
        eq_(DataSource.GUTENBERG, gutenberg.name)
        eq_(True, gutenberg.offers_licenses)

    def test_lookup_returns_none_for_nonexistent_source(self):
        eq_(None, DataSource.lookup(
            self._db, "No such data source " + self._str))

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

    def test_for_foreign_id_without_autocreate(self):
        identifier_type = WorkIdentifier.ISBN
        isbn = self._str

        # We don't want to auto-create a database record, so we set
        # autocreate=False
        identifier, was_new = WorkIdentifier.for_foreign_id(
            self._db, identifier_type, isbn, autocreate=False)
        eq_(None, identifier)
        eq_(False, was_new)


class TestContributor(DatabaseTest):

    def test_lookup_by_viaf(self):

        # Two contributors named Bob.
        bob1, new = Contributor.lookup(self._db, name="Bob", viaf="foo")
        bob2, new = Contributor.lookup(self._db, name="Bob", viaf="bar")

        assert bob1 != bob2

        eq_((bob1, False), Contributor.lookup(self._db, viaf="foo"))

    def test_lookup_by_lc(self):

        # Two contributors named Bob.
        bob1, new = Contributor.lookup(self._db, name="Bob", lc="foo")
        bob2, new = Contributor.lookup(self._db, name="Bob", lc="bar")

        assert bob1 != bob2

        eq_((bob1, False), Contributor.lookup(self._db, lc="foo"))

    def test_lookup_by_name(self):

        # Two contributors named Bob.
        bob1, new = Contributor.lookup(self._db, name="Bob", lc="foo")
        bob2, new = Contributor.lookup(self._db, name="Bob", lc="bar")

        # Lookup by name finds both of them.
        bobs, new = Contributor.lookup(self._db, name="Bob")
        eq_(False, new)
        eq_(["Bob", "Bob"], [x.name for x in bobs])

    def test_create_by_lookup(self):
        [bob1], new = Contributor.lookup(self._db, name="Bob")
        eq_("Bob", bob1.name)
        eq_(True, new)

        [bob2], new = Contributor.lookup(self._db, name="Bob")
        eq_(bob1, bob2)
        eq_(False, new)

    def test_merge(self):

        # Here's Robert.
        [robert], ignore = Contributor.lookup(self._db, name="Robert")
        
        # Here's Bob.
        [bob], ignore = Contributor.lookup(self._db, name="Bob")
        bob.extra['foo'] = 'bar'
        bob.aliases = ['Bobby']

        # Each is a contributor to a WorkRecord.
        data_source = DataSource.lookup(self._db, DataSource.GUTENBERG)

        roberts_book, ignore = WorkRecord.for_foreign_id(
            self._db, data_source, WorkIdentifier.GUTENBERG_ID, "1")
        roberts_book.add_contributor(robert, Contributor.AUTHOR_ROLE)

        bobs_book, ignore = WorkRecord.for_foreign_id(
            self._db, data_source, WorkIdentifier.GUTENBERG_ID, "10")
        bobs_book.add_contributor(bob, Contributor.AUTHOR_ROLE)

        # In a shocking turn of events, it transpires that "Bob" and
        # "Robert" are the same person. We merge "Bob" into Roberg
        # thusly:
        bob.merge_into(robert)

        # 'Bob' is now listed as an alias for Robert, as is Bob's
        # alias.
        eq_(['Bob', 'Bobby'], robert.aliases)

        # The extra information associated with Bob is now associated
        # with Robert.
        eq_('bar', robert.extra['foo'])

        # The standalone 'Bob' record has been removed from the database.
        eq_(
            [], 
            self._db.query(Contributor).filter(Contributor.name=="Bob").all())

        # Bob's book is now associated with 'Robert', not the standalone
        # 'Bob' record.
        eq_([robert], bobs_book.authors)

    def _names(self, in_name, out_family, out_display,
               default_display_name=None):
        f, d = Contributor._default_names(in_name, default_display_name)
        eq_(f, out_family)
        eq_(d, out_display)

    def test_default_names(self):

        # Pass in a default display name and it will always be used.
        self._names("Jones, Bob", "Jones", "Sally Smith",
                    default_display_name="Sally Smith")

        # Corporate names are untouched and get no family name.
        self._names("Bob's Books.", None, "Bob's Books.")
        self._names("Bob's Books, Inc.", None, "Bob's Books, Inc.")
        self._names("Little, Brown &amp; Co.", None, "Little, Brown & Co.")
        self._names("Philadelphia Broad Street Church (Philadelphia, Pa.)",
                    None, "Philadelphia Broad Street Church")

        # Dates and other gibberish after a name is removed.
        self._names("Twain, Mark, 1855-1910", "Twain", "Mark Twain")
        self._names("Twain, Mark, ???-1910", "Twain", "Mark Twain")
        self._names("Twain, Mark, circ. 1900", "Twain", "Mark Twain")
        self._names("Twain, Mark, !@#!@", "Twain", "Mark Twain")

        # Suffixes stay on the end, except for "Mrs.", which goes
        # to the front.
        self._names("Twain, Mark, Jr.", "Twain", "Mark Twain, Jr.")
        self._names("Twain, Mark, Mrs.", "Twain", "Mrs. Mark Twain")
        self._names("Twain, Mark, Mrs", "Twain", "Mrs Mark Twain")

        # The easy case.
        self._names("Twain, Mark", "Twain", "Mark Twain")
        self._names("Geering, R. G.", "Geering", "R. G. Geering")

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
        eq_(set([identifier.id]), record.equivalent_identifier_ids())

        # We can get the same work record by providing only the name
        # of the data source.
        record, was_new = WorkRecord.for_foreign_id(
            self._db, DataSource.GUTENBERG, type, id)
        eq_(data_source, record.data_source)
        eq_(identifier, record.primary_identifier)
        eq_(False, was_new)

    def test_missing_coverage_from(self):
        gutenberg = DataSource.lookup(self._db, DataSource.GUTENBERG)
        oclc = DataSource.lookup(self._db, DataSource.OCLC)
        web = DataSource.lookup(self._db, DataSource.WEB)

        # Here are two Gutenberg records.
        g1, ignore = WorkRecord.for_foreign_id(
            self._db, gutenberg, WorkIdentifier.GUTENBERG_ID, "1")

        g2, ignore = WorkRecord.for_foreign_id(
            self._db, gutenberg, WorkIdentifier.GUTENBERG_ID, "2")

        # One of them has coverage from OCLC Classify
        c1 = self._coverage_record(g1, oclc)

        # Here's a web record, just sitting there.
        w, ignore = WorkRecord.for_foreign_id(
            self._db, web, WorkIdentifier.URI, "http://www.foo.com/")

        # missing_coverage_from picks up the Gutenberg record with no
        # coverage from OCLC. It doesn't pick up the other
        # Gutenberg record, and it doesn't pick up the web record.
        [in_gutenberg_but_not_in_oclc] = WorkRecord.missing_coverage_from(
            self._db, gutenberg, oclc).all()

        eq_(g2, in_gutenberg_but_not_in_oclc)

        # We don't put web sites into OCLC, so this will pick up the
        # web record (but not the Gutenberg record).
        [in_web_but_not_in_oclc] = WorkRecord.missing_coverage_from(
            self._db, web, oclc).all()
        eq_(w, in_web_but_not_in_oclc)

        # We don't use the web as a source of coverage, so this will
        # return both Gutenberg records (but not the web record).
        eq_([g1.id, g2.id], sorted([x.id for x in WorkRecord.missing_coverage_from(
            self._db, gutenberg, web)]))

    def test_recursive_workrecord_equivalence(self):

        gutenberg_source = DataSource.lookup(self._db, DataSource.GUTENBERG)
        open_library_source = DataSource.lookup(self._db, DataSource.OPEN_LIBRARY)
        web_source = DataSource.lookup(self._db, DataSource.WEB)

        # Here's a WorkRecord for a Project Gutenberg text.
        gutenberg, ignore = WorkRecord.for_foreign_id(
            self._db, gutenberg_source, WorkIdentifier.GUTENBERG_ID, "1")
        gutenberg.title = "Original Gutenberg text"

        # Here's a WorkRecord for an Open Library text.
        open_library, ignore = WorkRecord.for_foreign_id(
            self._db, open_library_source, WorkIdentifier.OPEN_LIBRARY_ID,
            "W1111")
        open_library.title = "Open Library record"

        # We've learned from OCLC Classify that the Gutenberg text is
        # equivalent to a certain OCLC Number. We've learned from OCLC
        # Linked Data that the Open Library text is equivalent to the
        # same OCLC Number.
        oclc_classify = DataSource.lookup(self._db, DataSource.OCLC)
        oclc_linked_data = DataSource.lookup(self._db, DataSource.OCLC_LINKED_DATA)

        oclc_number, ignore = WorkIdentifier.for_foreign_id(
            self._db, WorkIdentifier.OCLC_NUMBER, "22")
        gutenberg.primary_identifier.equivalent_to(
            oclc_classify, oclc_number, 1)
        open_library.primary_identifier.equivalent_to(
            oclc_linked_data, oclc_number, 1)
       
        # Here's a WorkRecord for a Recovering the Classics cover.
        recovering, ignore = WorkRecord.for_foreign_id(
            self._db, web_source, WorkIdentifier.URI, 
            "http://recoveringtheclassics.com/pride-and-prejudice.jpg")
        recovering.title = "Recovering the Classics cover"

        # We've manually associated that WorkRecord's URI directly
        # with the Project Gutenberg text.
        manual = DataSource.lookup(self._db, DataSource.MANUAL)
        gutenberg.primary_identifier.equivalent_to(
            manual, recovering.primary_identifier, 1)

        # Finally, here's a completely unrelated WorkRecord, which
        # will not be showing up.
        gutenberg2, ignore = WorkRecord.for_foreign_id(
            self._db, gutenberg_source, WorkIdentifier.GUTENBERG_ID, "2")
        gutenberg2.title = "Unrelated Gutenberg record."

        # When we call equivalent_workrecords on the Project Gutenberg
        # WorkRecord, we get three WorkRecords: the Gutenberg record
        # itself, the Open Library record, and the Recovering the
        # Classics record.
        #
        # We get the Open Library record because it's associated with
        # the same OCLC Number as the Gutenberg record. We get the
        # Recovering the Classics record because it's associated
        # directly with the Gutenberg record.
        results = list(gutenberg.equivalent_work_records())
        eq_(3, len(results))
        assert gutenberg in results
        assert open_library in results
        assert recovering in results

        # Here's a Work that incorporates one of the Gutenberg records.
        work = Work()
        work.work_records.extend([gutenberg2])

        # Its set-of-all-workrecords contains only one record.
        eq_(1, work.all_workrecords().count())

        # If we add the other Gutenberg record to it, then its
        # set-of-all-workrecords is extended by that record, *plus*
        # all the WorkRecords equivalent to that record.
        work.work_records.extend([gutenberg])
        eq_(4, work.all_workrecords().count())


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

    def test_with_no_work(self):
        p1, ignore = LicensePool.for_foreign_id(
            self._db, DataSource.GUTENBERG, WorkIdentifier.GUTENBERG_ID, "1")

        p2, ignore = LicensePool.for_foreign_id(
            self._db, DataSource.OVERDRIVE, WorkIdentifier.OVERDRIVE_ID, "2")

        work, ignore = get_one_or_create(self._db, Work, title="Foo")
        p1.work = work
        
        assert p1 in work.license_pools

        eq_([p2], LicensePool.with_no_work(self._db))

class TestWork(DatabaseTest):

    def test_calculate_presentation(self):

        gutenberg_source = DataSource.GUTENBERG

        [bob], ignore = Contributor.lookup(self._db, "Bitshifter, Bob")
        bob.family_name, bob.display_name = bob.default_names()

        wr1, pool1 = self._workrecord(
            gutenberg_source, WorkIdentifier.GUTENBERG_ID, True)
        wr1.title = "The 1st Title"
        wr1.add_contributor(bob, Contributor.AUTHOR_ROLE)

        wr2, pool2 = self._workrecord(
            gutenberg_source, WorkIdentifier.GUTENBERG_ID, True)
        wr2.title = "The 2nd Title"
        wr2.add_contributor(bob, Contributor.AUTHOR_ROLE)
        wr2.add_contributor("Adder, Alice", Contributor.AUTHOR_ROLE)

        wr3, pool3 = self._workrecord(
            gutenberg_source, WorkIdentifier.GUTENBERG_ID, True)
        wr3.title = "The 2nd Title"
        wr3.add_contributor(bob, Contributor.AUTHOR_ROLE)
        wr3.add_contributor("Adder, Alice", Contributor.AUTHOR_ROLE)

        work = Work()
        for i in wr1, wr2, wr3:
            work.work_records.append(i)
        for p in pool1, pool2, pool3:
            work.license_pools.append(p)

        # The title of the Work is the most common title among
        # its associated WorkRecords.
        eq_(None, work.title)
        work.calculate_presentation()
        eq_("The 2nd Title", work.title)
        eq_("2nd Title, The", work.sort_title)

        # Bob was listed as an author for all three WorkRecords,
        # making him the most popular author, so he's listed as the
        # author of the work.
        #
        # TODO: We currently can't handle multiple authors. This needs
        # to be fixed.
        eq_("Bob Bitshifter", work.authors)
        eq_("Bitshifter, Bob", work.sort_author)

class TestLane(DatabaseTest):

    def setup(self):
        super(TestLane, self).setup()
        self.lanes = LaneList.from_description(
            self._db,
            [dict(name="Fiction",
                  fiction=True,
                  audience=Classifier.AUDIENCE_ADULT,
                  genres=[]),
             Fantasy,
             dict(
                 name="Young Adult",
                 fiction=Lane.BOTH_FICTION_AND_NONFICTION,
                 audience=Classifier.AUDIENCE_YOUNG_ADULT,
                 genres=[]),
         ]
        )


    def test_quality_sample_genre_filter(self):

        english = "eng"
        genre = Fantasy

        # Here's a high-quality work.
        w1 = self._work(language=english, genre=genre, quality=100)

        # Here's a medium-quality-work.
        w2 = self._work(language=english, genre=genre, quality=10)

        # Here's a low-quality work.
        w3 = self._work(language=english, genre=genre, quality=1)

        # Here's a work of abysmal quality.
        w4 = self._work(language=english, genre=genre, quality=0)

        # We want two works of quality at least 200, but we'll settle
        # for quality 50. Even that is too much to ask, and we end up with
        # only one work that fits the criteria.
        eq_([w1], Work.quality_sample(self._db, english, genre, 200, 50, 2))

        # We want two works of quality at least 50, but we'll settle
        # for quality 10. This gives us the 100 and the 10.
        eq_([w1, w2], Work.quality_sample(self._db, english, genre, 50, 10, 2))

        # We want ten works of quality at least one, but less than
        # zero. This gives us everything except the zero.
        eq_(set([w1, w2, w3]), set(Work.quality_sample(
            self._db, english, genre, 1, 0.000001, 10)))

        # We want ten works of quality of at least 50, nothing less.
        # We only get one work.
        eq_([w1], Work.quality_sample(self._db, english, genre, 50, 50, 10))


    def test_quality_sample_language_filter(self):
        w1 = self._work(language="eng", genre=Fantasy)
        w1.quality = 100
        w2 = self._work(language="spa", genre=Fantasy)
        w2.quality = 100

        eq_([w1], Work.quality_sample(self._db, "eng", Fantasy, 0, 0, 2))
        eq_([w2], Work.quality_sample(self._db, "spa", Fantasy, 0, 0, 2))
        eq_([], Work.quality_sample(self._db, "fre", Fantasy, 0, 0, 2))
        eq_(set([w1, w2]), set(
            Work.quality_sample(self._db, ["eng", "spa"], Fantasy, 0, 0, 2)))

    def test_quality_sample_genre_filter(self):
        w1 = self._work(language="eng", genre=Fantasy)
        w1.quality = 100
        w2 = self._work(language="eng", genre=Romance)
        w2.quality = 100

        eq_([w1], Work.quality_sample(self._db, "eng", Fantasy, 0, 0, 2))
        eq_([w2], Work.quality_sample(self._db, "eng", Romance, 0, 0, 2))
        eq_([], Work.quality_sample(self._db, "eng", Drama, 0, 0, 2))


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

class TestWorkQuality(DatabaseTest):

    # TODO: More summaries and more covers gives you a higher rating.

    def test_better_known_work_gets_higher_rating(self):

        gutenberg_source = DataSource.lookup(self._db, DataSource.GUTENBERG)

        wr1_1, pool1 = self._workrecord(with_license_pool=True)
        wr1_2 = self._workrecord(with_license_pool=False)

        wr2_1, pool2 = self._workrecord(with_license_pool=True)

        work1 = Work()
        work1.work_records.extend([wr1_1, wr1_2])
        work1.license_pools.extend([pool1])

        work2 = Work()
        work2.work_records.extend([wr2_1])
        work2.license_pools.extend([pool2])

        work1.calculate_presentation()
        work2.calculate_presentation()

        assert work1.quality > work2.quality

    def test_more_license_pools_gets_higher_rating(self):

        gutenberg_source = DataSource.lookup(self._db, DataSource.GUTENBERG)

        wr1_1, pool1 = self._workrecord(with_license_pool=True)
        wr1_2, pool2 = self._workrecord(with_license_pool=True)

        wr2_1, pool3 = self._workrecord(with_license_pool=True)
        wr2_2 = self._workrecord(with_license_pool=False)

        work1 = Work()
        work1.work_records.extend([wr1_1, wr1_2])
        work1.license_pools.extend([pool1, pool2])

        work2 = Work()
        work2.work_records.extend([wr2_1, wr2_2])
        work2.license_pools.extend([pool3])

        work1.calculate_presentation()
        work2.calculate_presentation()

        assert work1.quality > work2.quality

class TestWorkSimilarity(DatabaseTest):

    def test_work_is_similar_to_itself(self):
        wr = self._workrecord()
        eq_(1, wr.similarity_to(wr))

class TestWorkConsolidation(DatabaseTest):

    # Versions of Work and WorkRecord instrumented to bypass the
    # normal similarity comparison process.

    def setup(self):
        super(TestWorkConsolidation, self).setup()
        # Replace the complex implementations of similarity_to with 
        # much simpler versions that let us simply say which objects 
        # are to be considered similar.
        def similarity_to(self, other):
            if other in getattr(self, 'similar', []):
                return 1
            return 0
        self.old_w = Work.similarity_to
        self.old_wr = WorkRecord.similarity_to
        Work.similarity_to = similarity_to
        WorkRecord.similarity_to = similarity_to

    def teardown(self):
        Work.similarity_to = self.old_w
        WorkRecord.similarity_to = self.old_wr
        super(TestWorkConsolidation, self).teardown()

    def test_calculate_work_for_licensepool_where_primary_work_record_has_work(self):
        # This is the easy case.
        args = [self._db, DataSource.GUTENBERG, WorkIdentifier.GUTENBERG_ID,
                "1"]
        # Here's a LicensePool for a book from Gutenberg.
        license, ignore = LicensePool.for_foreign_id(*args)

        # Here's a WorkRecord for the same Gutenberg book.
        work_record, ignore = WorkRecord.for_foreign_id(*args)

        # The WorkRecord has a Work associated with it.
        work = Work()
        work_record.work = work

        eq_(None, license.work)
        license.calculate_work()

        # Now, the LicensePool has the same Work associated with it.
        eq_(work, license.work)

    def test_calculate_work_for_licensepool_creates_new_work(self):

        # This work record is unique to the existing work.
        wr1, ignore = WorkRecord.for_foreign_id(
            self._db, DataSource.GUTENBERG, WorkIdentifier.GUTENBERG_ID, "1")
        preexisting_work = Work()
        preexisting_work.work_records = [wr1]

        # This work record is unique to the new LicensePool
        wr2, ignore = WorkRecord.for_foreign_id(
            self._db, DataSource.GUTENBERG, WorkIdentifier.GUTENBERG_ID, "3")
        pool, ignore = LicensePool.for_foreign_id(
            self._db, DataSource.GUTENBERG, WorkIdentifier.GUTENBERG_ID, "3")

        work, created = pool.calculate_work()
        eq_(True, created)
        assert work != preexisting_work

    def test_calculate_work_for_new_work(self):
        # TODO: This test doesn't actually test
        # anything. calculate_work() is too complicated and needs to
        # be refactored.

        # This work record is unique to the existing work.
        wr1, ignore = WorkRecord.for_foreign_id(
            self._db, DataSource.GUTENBERG, WorkIdentifier.GUTENBERG_ID, "1")

        # This work record is shared by the existing work and the new
        # LicensePool.
        wr2, ignore = WorkRecord.for_foreign_id(
            self._db, DataSource.GUTENBERG, WorkIdentifier.GUTENBERG_ID, "2")

        # These work records are unique to the new LicensePool.

        wr3, ignore = WorkRecord.for_foreign_id(
            self._db, DataSource.GUTENBERG, WorkIdentifier.GUTENBERG_ID, "3")

        wr4, ignore = WorkRecord.for_foreign_id(
            self._db, DataSource.GUTENBERG, WorkIdentifier.GUTENBERG_ID, "4")

        # Make wr4's primary identifier equivalent to wr3's and wr1's
        # primaries.
        data_source = DataSource.lookup(self._db, DataSource.OCLC_LINKED_DATA)
        for make_equivalent in wr3, wr1:
            wr4.primary_identifier.equivalent_to(
                data_source, make_equivalent.primary_identifier, 1)
        preexisting_work = Work()
        preexisting_work.work_records.extend([wr1, wr2])

        pool, ignore = LicensePool.for_foreign_id(
            self._db, DataSource.GUTENBERG, WorkIdentifier.GUTENBERG_ID, "4")
        self._db.commit()

        pool.calculate_work()

    def test_merge_into(self):

        # Here's a work with a license pool and two work records.
        work_record_1a, pool_1a = self._workrecord(
            DataSource.OCLC, WorkIdentifier.OCLC_WORK, True)
        work_record_1b, ignore = WorkRecord.for_foreign_id(
            self._db, DataSource.OCLC, WorkIdentifier.OCLC_WORK, "W2")

        work1 = Work()
        work1.license_pools = [pool_1a]
        work1.work_records = [work_record_1a, work_record_1b]

        # Here's a work with two license pools and one work record
        work_record_2a, pool_2a = self._workrecord(
            DataSource.GUTENBERG, WorkIdentifier.GUTENBERG_ID, True)
        work_record_2a.title = "The only title in this whole test."
        pool_2b = self._licensepool(work_record_2a, DataSource.OCLC)

        work2 = Work()
        work2.license_pools = [pool_2a, pool_2b]
        work2.work_records = [work_record_2a]

        self._db.commit()

        # This attempt to merge the two work records will fail because
        # they don't meet the similarity threshold.
        work2.merge_into(work1, similarity_threshold=1)
        eq_(None, work2.was_merged_into)

        # This attempt will succeed because we lower the similarity
        # threshold.
        work2.merge_into(work1, similarity_threshold=0)
        eq_(work1, work2.was_merged_into)

        # The merged Work no longer has any work records or license
        # pools.
        eq_([], work2.work_records)
        eq_([], work2.license_pools)


        # The remaining Work has all three license pools.
        for p in pool_1a, pool_2a, pool_2b:
            assert p in work1.license_pools

        # It has all three work records.
        for w in work_record_1a, work_record_1b, work_record_2a:
            assert w in work1.work_records

        # Its presentation has been updated and its title now comes from
        # one of the now-deleted Work's WorkRecords.
        eq_("The only title in this whole test.", work1.title)


class TestLoans(DatabaseTest):

    def test_open_access_loan(self):
        patron = self._patron()
        work = self._work(with_license_pool=True)
        pool = work.license_pools[0]
        pool.is_open_access = True

        # The patron has no active loans.
        eq_([], patron.loans)

        # Loan them the book
        loan, was_new = pool.loan_to(patron)

        # Now they have a loan!
        eq_([loan], patron.loans)
        eq_(loan.patron, patron)
        eq_(loan.license_pool, pool)
        assert (datetime.datetime.utcnow() - loan.start) < datetime.timedelta(seconds=1)

        # TODO: At some future point it may be relevant that loan.end
        # is None here, but before that happens the loan process will
        # become more complicated, so there's no point in writing
        # a bunch of test code now.

        # Try getting another loan for this book.
        loan2, was_new = pool.loan_to(patron)

        # They're the same!
        eq_(loan, loan2)
        eq_(False, was_new)

class TestLane(DatabaseTest):

    def test_setup(self):
        fantasy_genre, ignore = Genre.lookup(
            self._db, classifier.Fantasy)

        fantasy_lane = Lane(
            self._db, fantasy_genre.name, 
            [fantasy_genre], True, Lane.FICTION_DEFAULT_FOR_GENRE,
            Classifier.AUDIENCE_ADULT)

        eq_([fantasy_genre], fantasy_lane.genres)
        eq_(Classifier.AUDIENCE_ADULT, fantasy_lane.audience)
        eq_(Lane.FICTION_DEFAULT_FOR_GENRE, fantasy_lane.fiction)
        eq_(True, fantasy_lane.include_subgenres)


class TestLaneList(DatabaseTest):
    
    def test_from_description(self):
        lanes = LaneList.from_description(
            self._db,
            [dict(name="Fiction",
                  fiction=True,
                  audience=Classifier.AUDIENCE_ADULT,
                  genres=[]),
             Fantasy,
             dict(
                 name="Young Adult",
                 fiction=Lane.BOTH_FICTION_AND_NONFICTION,
                 audience=Classifier.AUDIENCE_YOUNG_ADULT,
                 genres=[]),
         ]
        )

        fantasy_genre, ignore = Genre.lookup(self._db, Fantasy.name)

        fiction = lanes.by_name['Fiction']
        young_adult = lanes.by_name['Young Adult']
        fantasy = lanes.by_name['Fantasy'] 

        eq_(set([fantasy, fiction, young_adult]), set(lanes.lanes))

        eq_("Fiction", fiction.name)
        eq_(Classifier.AUDIENCE_ADULT, fiction.audience)
        eq_([], fiction.genres)
        eq_(True, fiction.fiction)

        eq_("Fantasy", fantasy.name)
        eq_(Classifier.AUDIENCE_ADULT, fantasy.audience)
        eq_([fantasy_genre], fantasy.genres)
        eq_(True, fantasy.fiction)

        eq_("Young Adult", young_adult.name)
        eq_(Classifier.AUDIENCE_YOUNG_ADULT, young_adult.audience)
        eq_([], young_adult.genres)
        eq_(Lane.BOTH_FICTION_AND_NONFICTION, young_adult.fiction)

class TestWorkFeed(DatabaseTest):

    def setup(self):
        super(TestWorkFeed, self).setup()
        self.fantasy_genre, ignore = Genre.lookup(
            self._db, classifier.Fantasy)
        self.fantasy_lane = Lane(
            self._db, self.fantasy_genre.name, 
            [self.fantasy_genre], True, Lane.FICTION_DEFAULT_FOR_GENRE,
            Classifier.AUDIENCE_ADULT)

    def test_setup(self):
        by_author = WorkFeed(self.fantasy_lane, "eng", order_by=Work.authors)

        eq_(["eng"], by_author.languages)
        eq_(self.fantasy_lane, by_author.lane)
        eq_([Work.authors, Work.title, Work.id], by_author.order_by)

        by_title = WorkFeed(self.fantasy_lane, ["eng", "spa"],
                            order_by=Work.title)
        eq_(["eng", "spa"], by_title.languages)
        eq_([Work.title, Work.authors, Work.id], by_title.order_by)

    def test_several_books_same_author(self):
        title = "The Title"
        author = "Author, The"
        language = ["eng"]
        genre = self.fantasy_genre
        audience = Classifier.AUDIENCE_ADULT

        # We've got three works with the same author but different
        # titles, plus one with a different author and title.
        w1 = self._work("Title B", author, genre, language, audience, 
                        with_license_pool=True)
        w2 = self._work("Title A", author, genre, language, audience, 
                        with_license_pool=True)
        w3 = self._work("Title C", author, genre, language, audience, 
                        with_license_pool=True)
        w4 = self._work("Title D", "Author, Another", genre, language, 
                        audience, with_license_pool=True)

        # Order them by title, and everything's fine.
        feed = WorkFeed(self.fantasy_lane, language, order_by=Work.title)
        eq_([w2, w1, w3, w4], feed.page_query(self._db, None, 10).all())
        eq_([w3, w4], feed.page_query(self._db, w1, 10).all())

        # Order them by author, and they're secondarily ordered by title.
        feed = WorkFeed(self.fantasy_lane, language, order_by=Work.authors)
        eq_([w4, w2, w1, w3], feed.page_query(self._db, None, 10).all())
        eq_([w3], feed.page_query(self._db, w1, 10).all())

        eq_([], feed.page_query(self._db, w3, 10).all())

    def test_several_books_same_author(self):
        title = "The Title"
        language = "eng"
        genre = self.fantasy_genre
        lane = self.fantasy_lane
        audience = Classifier.AUDIENCE_ADULT
        
        # We've got three works with the same author but different
        # titles, plus one with a different author and title.
        w1 = self._work(title, "Author B", genre, language, audience,
                        with_license_pool=True)
        w2 = self._work(title, "Author A", genre, language, audience, 
                        with_license_pool=True)
        w3 = self._work(title, "Author C", genre, language, audience, 
                        with_license_pool=True)
        w4 = self._work("Different title", "Author D", genre, language, 
                        with_license_pool=True)

        # Order them by author, and everything's fine.
        feed = WorkFeed(lane, language, order_by=Work.authors)
        eq_([w2, w1, w3, w4], feed.page_query(self._db, None, 10).all())
        eq_([w3, w4], feed.page_query(self._db, w1, 10).all())

        # Order them by title, and they're secondarily ordered by author.
        feed = WorkFeed(lane, language, order_by=Work.title)
        eq_([w4, w2, w1, w3], feed.page_query(self._db, None, 10).all())
        eq_([w3], feed.page_query(self._db, w1, 10).all())

        eq_([], feed.page_query(self._db, w3, 10).all())

    def test_several_books_same_author_and_title(self):
        
        title = "The Title"
        author = "Author, The"
        language = "eng"
        genre = self.fantasy_genre
        lane = self.fantasy_lane
        audience = Classifier.AUDIENCE_ADULT

        # We've got four works with the exact same title and author
        # string.
        w1, w2, w3, w4 = [
            self._work(title, author, genre, language, audience,
                       with_license_pool=True)
            for i in range(4)]

        # WorkFeed orders them by ID.
        feed = WorkFeed(lane, language, order_by=Work.authors)
        query = feed.page_query(self._db, None, 10)
        eq_([w1, w2, w3, w4], query.all())

        # If we provide a last seen work, we only get the works
        # after that one.
        query = feed.page_query(self._db, w2, 10)
        eq_([w3, w4], query.all())

        eq_([], feed.page_query(self._db, w4, 10).all())


class TestCoverageProvider(DatabaseTest):

    class AlwaysSuccessful(CoverageProvider):
        def process_work_record(self, work_record):
            return True

    class NeverSuccessful(CoverageProvider):
        def process_work_record(self, work_record):
            return False

    def setup(self):
        super(TestCoverageProvider, self).setup()
        self.input_source = DataSource.lookup(self._db, DataSource.GUTENBERG)
        self.output_source = DataSource.lookup(self._db, DataSource.OCLC)
        self.work_record = self._workrecord(self.input_source.name)

    def test_always_successful(self):

        # We start with no CoverageRecords and no Timestamp.
        eq_([], self._db.query(CoverageRecord).all())
        eq_([], self._db.query(Timestamp).all())

        provider = self.AlwaysSuccessful(
            "Always successful", self.input_source, self.output_source)
        provider.run()

        # There is now one CoverageRecord
        [record] = self._db.query(CoverageRecord).all()
        eq_(self.work_record, record.work_record)
        eq_(self.output_source, self.output_source)

        # The timestamp is now set.
        [timestamp] = self._db.query(Timestamp).all()
        eq_("Always successful", timestamp.service)


    def test_never_successful(self):

        # We start with no CoverageRecords and no Timestamp.
        eq_([], self._db.query(CoverageRecord).all())
        eq_([], self._db.query(Timestamp).all())

        provider = self.NeverSuccessful(
            "Never successful", self.input_source, self.output_source)
        provider.run()

        # There is still no CoverageRecord
        eq_([], self._db.query(CoverageRecord).all())

        # But the coverage provider did run, and the timestamp is now set.
        [timestamp] = self._db.query(Timestamp).all()
        eq_("Never successful", timestamp.service)

