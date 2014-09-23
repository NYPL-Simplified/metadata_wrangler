from nose.tools import set_trace, eq_
import datetime
import pkgutil
from model import (
    CirculationEvent,
    Contributor,
    DataSource,
    LicensePool,
    Resource,
    WorkIdentifier,
    WorkRecord,
)
from integration.threem import (
    CirculationParser,
    EventParser,
    ItemListParser,
)

class Test3MEventParser(object):

    # Sample event feed to test out the parser.
    TWO_EVENTS = """<LibraryEventBatch xmlns:xsd="http://www.w3.org/2001/XMLSchema" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
  <PublishId>1b0d6667-a10e-424a-9f73-fb6f6d41308e</PublishId>
  <PublishDateTimeInUTC>2014-04-14T13:59:05.6920303Z</PublishDateTimeInUTC>
  <LastEventDateTimeInUTC>2014-04-03T00:00:34</LastEventDateTimeInUTC>
  <Events>
    <CloudLibraryEvent>
      <LibraryId>test-library</LibraryId>
      <EventId>event-1</EventId>
      <EventType>CHECKIN</EventType>
      <EventStartDateTimeInUTC>2014-04-03T00:00:23</EventStartDateTimeInUTC>
      <EventEndDateTimeInUTC>2014-04-03T00:00:23</EventEndDateTimeInUTC>
      <ItemId>theitem1</ItemId>
      <ISBN>900isbn1</ISBN>
      <PatronId>patronid1</PatronId>
      <EventPublishDateTimeInUTC>2014-04-14T13:59:05</EventPublishDateTimeInUTC>
    </CloudLibraryEvent>
    <CloudLibraryEvent>
      <LibraryId>test-library</LibraryId>
      <EventId>event-2</EventId>
      <EventType>CHECKOUT</EventType>
      <EventStartDateTimeInUTC>2014-04-03T00:00:34</EventStartDateTimeInUTC>
      <EventEndDateTimeInUTC>2014-04-02T23:57:37</EventEndDateTimeInUTC>
      <ItemId>theitem2</ItemId>
      <ISBN>900isbn2</ISBN>
      <PatronId>patronid2</PatronId>
      <EventPublishDateTimeInUTC>2014-04-14T13:59:05</EventPublishDateTimeInUTC>
    </CloudLibraryEvent>
  </Events>
</LibraryEventBatch>
"""

    def test_parse_event_batch(self):
        # Parsing the XML gives us two events.
        event1, event2 = EventParser().process_all(self.TWO_EVENTS)

        (threem_id, isbn, patron_id, start_time, end_time,
         internal_event_type) = event1

        eq_("theitem1", threem_id)
        eq_("900isbn1", isbn)
        eq_("patronid1", patron_id)
        eq_(CirculationEvent.CHECKIN, internal_event_type)
        eq_(start_time, end_time)

        (threem_id, isbn, patron_id, start_time, end_time,
         internal_event_type) = event2
        eq_("theitem2", threem_id)
        eq_("900isbn2", isbn)
        eq_("patronid2", patron_id)
        eq_(CirculationEvent.CHECKOUT, internal_event_type)

        # Verify that start and end time were parsed correctly.
        correct_start = datetime.datetime(2014, 4, 3, 0, 0, 34)
        correct_end = datetime.datetime(2014, 4, 2, 23, 57, 37)
        eq_(correct_start, start_time)
        eq_(correct_end, end_time)


class Test3MCirculationParser(object):

    # Sample circulation feed for testing the parser.

    TWO_CIRCULATION_STATUSES = """
<ArrayOfItemCirculation xmlns:xsd="http://www.w3.org/2001/XMLSchema" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
<ItemCirculation>
  <ItemId>item1</ItemId>
  <ISBN13>900isbn1</ISBN13>
  <TotalCopies>2</TotalCopies>
  <AvailableCopies>0</AvailableCopies>
  <Checkouts>
    <Patron>
      <PatronId>patron1</PatronId>
      <EventStartDateInUTC>2014-03-24T13:10:51</EventStartDateInUTC>
      <EventEndDateInUTC>2014-04-15T13:10:51</EventEndDateInUTC>
      <Position>1</Position>
    </Patron>
  </Checkouts>
  <Holds/>
  <Reserves>
    <Patron>
      <PatronId>patron2</PatronId>
      <EventStartDateInUTC>2014-03-24T13:10:51</EventStartDateInUTC>
      <EventEndDateInUTC>2014-04-15T13:10:51</EventEndDateInUTC>
      <Position>1</Position>
    </Patron>
  </Reserves>
</ItemCirculation>

<ItemCirculation>
  <ItemId>item2</ItemId>
  <ISBN13>900isbn2</ISBN13>
  <TotalCopies>1</TotalCopies>
  <AvailableCopies>0</AvailableCopies>
  <Checkouts>
    <Patron>
      <PatronId>patron3</PatronId>
      <EventStartDateInUTC>2014-04-23T22:14:02</EventStartDateInUTC>
      <EventEndDateInUTC>2014-05-14T22:14:02</EventEndDateInUTC>
      <Position>1</Position>
    </Patron>
  </Checkouts>
  <Holds>
    <Patron>
      <PatronId>patron4</PatronId>
      <EventStartDateInUTC>2014-04-24T18:10:44</EventStartDateInUTC>
      <EventEndDateInUTC>2014-04-24T18:11:02</EventEndDateInUTC>
      <Position>1</Position>
    </Patron>
  </Holds>
  <Reserves/>
</ItemCirculation>
</ArrayOfItemCirculation>
"""

    def test_parse_circulation_batch(self):
        event1, event2 = CirculationParser().process_all(
            self.TWO_CIRCULATION_STATUSES)

        eq_('item1', event1[WorkIdentifier][WorkIdentifier.THREEM_ID])
        eq_('900isbn1', event1[WorkIdentifier][WorkIdentifier.ISBN])
        eq_(2, event1[LicensePool.licenses_owned])
        eq_(0, event1[LicensePool.licenses_available])
        eq_(1, event1[LicensePool.licenses_reserved])
        eq_(0, event1[LicensePool.patrons_in_hold_queue])

        eq_('item2', event2[WorkIdentifier][WorkIdentifier.THREEM_ID])
        eq_('900isbn2', event2[WorkIdentifier][WorkIdentifier.ISBN])
        eq_(1, event2[LicensePool.licenses_owned])
        eq_(0, event2[LicensePool.licenses_available])
        eq_(0, event2[LicensePool.licenses_reserved])
        eq_(1, event2[LicensePool.patrons_in_hold_queue])


class TestItemListParser(object):

    def text_parse_author_string(cls):
        authors = ItemListParser.author_names_from_string(
            "Walsh, Jill Paton; Sayers, Dorothy L.")
        eq_(authors, ["Walsh, Jill Paton", "Sayers, Dorothy L."])

    def test_item_list(cls):
        data = pkgutil.get_data(
            "tests.integrate",
            "files/3m_item_metadata_list.xml")
        
        data = [(id, raw, cooked)
                for (id, raw,cooked)  in ItemListParser().parse(data)]

        # There should be 25 items in the list.
        eq_(25, len(data))

        # Do a spot check of the first item in the list
        id, raw, cooked = data[0]

        eq_("ddf4gr9", id)

        assert raw.startswith("<Item")

        eq_(id, cooked[WorkIdentifier][WorkIdentifier.THREEM_ID])
        eq_("9781250015280", cooked[WorkIdentifier][WorkIdentifier.ISBN])
        eq_("The Incense Game", cooked[WorkRecord.title])
        eq_("A Novel of Feudal Japan", cooked[WorkRecord.subtitle])
        eq_(["Rowland, Laura Joh"], cooked[Contributor])
        eq_("eng", cooked[WorkRecord.language])
        eq_("St. Martin's Press", cooked[WorkRecord.publisher])
        eq_("1.2 MB", cooked['extra']['fileSize'])
        eq_("304", cooked['extra']['numberOfPages'])
        eq_(datetime.datetime(year=2012, month=9, day=17), cooked[WorkRecord.published])

        summary = cooked[Resource][Resource.DESCRIPTION]
        assert summary.startswith("<b>Winner")

        # Check the links

        image = cooked[Resource][Resource.IMAGE]
        assert image.startswith("http://ebook.3m.com/delivery")

        alternate = cooked[Resource]["alternate"]
        assert alternate.startswith("http://ebook.3m.com/library")
